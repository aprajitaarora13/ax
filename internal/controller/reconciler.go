// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	// templateDigestBytes is how many bytes of the spec digest go into a
	// per-task ActorTemplate name.
	templateDigestBytes = 4
	// defaultWorkspaceReadyTimeout is how long Resume waits for a freshly
	// resumed actor to finish workspace setup.
	defaultWorkspaceReadyTimeout = 15 * time.Second
	workspaceReadyPollInterval   = 500 * time.Millisecond

	defaultServiceDNSCAFile      = "/run/servicedns-ca/trust-bundle.pem"
	defaultPodIdentityBundleFile = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
	defaultRouterServerName      = "atenet-router.ate-system.svc"
)

// TaskReconciler reconciles Task resources by provisioning and orchestrating
// sandboxed Actors on Agent Substrate.
type TaskReconciler struct {
	client                  *substrate.Client
	httpClient              *http.Client
	defaultTemplate         string
	defaultTemplateAtespace string
	// defaultTaskImage is applied to spec.image for tasks that leave it empty.
	defaultTaskImage string

	// WorkspaceReadyTimeout bounds how long Resume waits for the actor's workspace
	// to report ready before recording it as still initializing.
	WorkspaceReadyTimeout time.Duration
}

// NewTaskReconciler creates a new TaskReconciler. Empty defaults fall back to
// "default-template", v1alpha1.DefaultSystemNamespace and
// v1alpha1.DefaultTaskImage respectively.
func NewTaskReconciler(client *substrate.Client, defaultTemplate, defaultTemplateAtespace, defaultTaskImage string) *TaskReconciler {
	if defaultTemplate == "" {
		defaultTemplate = "default-template"
	}
	if defaultTemplateAtespace == "" {
		defaultTemplateAtespace = v1alpha1.DefaultSystemNamespace
	}
	if defaultTaskImage == "" {
		defaultTaskImage = v1alpha1.DefaultTaskImage
	}
	return &TaskReconciler{
		client:                  client,
		httpClient:              newProbeHTTPClient(),
		defaultTemplate:         defaultTemplate,
		defaultTemplateAtespace: defaultTemplateAtespace,
		defaultTaskImage:        defaultTaskImage,
		WorkspaceReadyTimeout:   defaultWorkspaceReadyTimeout,
	}
}

// Client returns the Substrate client used by the reconciler.
func (r *TaskReconciler) Client() *substrate.Client {
	return r.client
}

func newProbeHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: defaultRouterServerName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(defaultPodIdentityBundleFile, defaultPodIdentityBundleFile)
			if err != nil {
				return &tls.Certificate{}, nil
			}
			return &cert, nil
		},
	}
	if caBytes, err := os.ReadFile(defaultServiceDNSCAFile); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caBytes) {
			tlsCfg.RootCAs = pool
		}
	}
	transport.TLSClientConfig = tlsCfg
	return &http.Client{
		Timeout:   2 * time.Second,
		Transport: transport,
	}
}

// ensureSubstrateResources fills in the task's defaults and makes sure its
// atespace, ActorTemplate and actor exist on Substrate. It records only what AX
// stores in the task's status: the task ID, and on failure the reason in a
// False Ready condition. The caller sets the phase, and the server reads the
// rest of the status from the actor.
func (r *TaskReconciler) ensureSubstrateResources(ctx context.Context, task *v1alpha1.Task, ws *v1alpha1.Workspace) error {
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	atespace := task.Metadata.Atespace
	if atespace == "" {
		atespace = "default"
	}
	task.Metadata.Atespace = atespace

	if task.Spec == nil {
		task.Spec = &v1alpha1.TaskSpec{}
	}
	if task.Spec.Image == "" {
		task.Spec.Image = r.defaultTaskImage
	}

	slog.Info("reconciling task", "name", task.Metadata.Name, "atespace", atespace, "image", task.Spec.Image)

	now := time.Now()

	// 1. Ensure the Atespace exists in Substrate
	if err := r.client.EnsureAtespace(ctx, atespace); err != nil {
		if substrate.IsTransientError(err) {
			r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
			return fmt.Errorf("ensuring atespace: %w", err)
		}
		r.setNotReady(task, v1alpha1.ReasonAtespaceCreationFailed, err.Error(), now)
		return fmt.Errorf("ensuring atespace: %w", err)
	}

	// 2. The actor is always named after the task, so the two can be used
	// interchangeably (for example in the router's ate-target-actor header).
	actorName := task.Metadata.Name
	if task.Status.Id == "" {
		task.Status.Id = fmt.Sprintf("task-%s-%d", task.Metadata.Name, now.Unix())
	}

	// 3. Ensure Actor exists on Substrate
	templateName := r.defaultTemplate
	templateAtespace := r.defaultTemplateAtespace
	if parts := strings.SplitN(templateName, "/", 2); len(parts) == 2 {
		templateAtespace = parts[0]
		templateName = parts[1]
	}

	// Resource limits are enforced by the template, so they are checked
	// here as well as at apply time: a task must never run without limits it asked for.
	if err := v1alpha1.ValidateResources(task.Spec.Resources); err != nil {
		r.setNotReady(task, v1alpha1.ReasonInvalidResources, err.Error(), now)
		return fmt.Errorf("validating resources: %w", err)
	}
	resources := substrate.ResourceLimits(task.Spec.Resources)

	// The template carries no per-task data: no prompt, config, or env. Tasks
	// that run the same image with the same limits share one template (and so
	// its warm snapshots). The task itself is handed to the runner at runtime
	// by Resume (see deliverAssignment).
	if task.Spec != nil && task.Spec.Image != "" {
		sharedTemplateName := imageTemplateName(task.Spec.Image, task.Spec.Resources)
		slog.Info("ensuring shared ActorTemplate for image", "image", task.Spec.Image, "template", sharedTemplateName)
		tmpl, err := r.client.EnsureActorTemplateWithImage(ctx, templateAtespace, templateName, atespace, sharedTemplateName, task.Spec.Image, resources)
		if err != nil {
			if substrate.IsTransientError(err) {
				r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
				return fmt.Errorf("ensuring actor template: %w", err)
			}
			// Falling back to the default template would run a different image than
			// the user asked for, without the limits it asked for, or fail later with
			// a misleading "actor template not found". Surface the real error on the
			// task and stop here.
			r.setNotReady(task, v1alpha1.ReasonTemplateCreationFailed, substrateErrorMessage(err), now)
			return fmt.Errorf("ensuring actor template: %w", err)
		}
		if tmpl != nil && tmpl.Metadata != nil {
			templateAtespace = tmpl.Metadata.Atespace
			templateName = tmpl.Metadata.Name
			slog.Info("using shared ActorTemplate for actor", "templateAtespace", templateAtespace, "templateName", templateName)
		}
	}

	_, err := r.client.EnsureActor(ctx, atespace, actorName, templateAtespace, templateName)
	if err != nil {
		if substrate.IsTransientError(err) {
			r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
			return fmt.Errorf("ensuring actor: %w", err)
		}
		r.setNotReady(task, v1alpha1.ReasonActorCreationFailed, err.Error(), now)
		return fmt.Errorf("ensuring actor: %w", err)
	}
	return nil
}

// Suspend runs ensureSubstrateResources, then suspends the task's actor. A
// failure to suspend the actor is returned without changing the task's status.
func (r *TaskReconciler) Suspend(ctx context.Context, task *v1alpha1.Task, ws *v1alpha1.Workspace) error {
	if err := r.ensureSubstrateResources(ctx, task, ws); err != nil {
		return err
	}
	atespace, actorName := task.Metadata.Atespace, task.Metadata.Name
	slog.Info("suspending actor on Substrate", "actor", actorName)
	if err := r.client.SuspendActor(ctx, atespace, actorName); err != nil {
		return fmt.Errorf("suspending actor: %w", err)
	}
	slog.Info("task successfully suspended", "name", task.Metadata.Name, "actor", actorName)
	return nil
}

// Resume runs ensureSubstrateResources, resumes the task's actor, and waits up
// to WorkspaceReadyTimeout for the task's workspace to be set up, recording the
// result in the WorkspaceReady condition. A failure to resume the actor is
// recorded the way ensureSubstrateResources records its own.
func (r *TaskReconciler) Resume(ctx context.Context, task *v1alpha1.Task, ws *v1alpha1.Workspace) error {
	if err := r.ensureSubstrateResources(ctx, task, ws); err != nil {
		return err
	}
	atespace, actorName := task.Metadata.Atespace, task.Metadata.Name
	now := time.Now()

	// Resume the Actor to activate container execution
	slog.Info("resuming actor on Substrate worker", "actor", actorName)
	_, workerIP, err := r.client.ResumeActor(ctx, atespace, actorName)
	if err != nil {
		if substrate.IsTransientError(err) {
			r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
			return fmt.Errorf("resuming actor: %w", err)
		}
		r.setNotReady(task, v1alpha1.ReasonActorResumeFailed, err.Error(), now)
		return fmt.Errorf("resuming actor: %w", err)
	}

	// Check if workspace setup inside the actor has completed
	host := workerIP
	port := "80"
	if h, p, err := net.SplitHostPort(workerIP); err == nil {
		host = h
		port = p
	}
	readyPath := "/readyz?check=workspace"
	// The task is handed to the runner at runtime instead of through the
	// template, so every resume delivers it once the runner answers. The runner
	// treats a repeat delivery of the same task as a no-op.
	assignment, err := encodeAssignment(task, ws)
	if err != nil {
		r.setNotReady(task, v1alpha1.ReasonActorResumeFailed, err.Error(), now)
		return fmt.Errorf("encoding assignment: %w", err)
	}
	delivered := false
	// Workspace setup happens once per task. After it has completed, WorkspaceReady stays
	// True across suspend/resume cycles, so only poll while it is still initializing.
	workspaceReady := conditionTrue(task, v1alpha1.ConditionWorkspaceReady)
	if workerIP != "" {
		// Poll briefly for assignment delivery and workspace setup completion
		pollCtx, cancel := context.WithTimeout(ctx, r.WorkspaceReadyTimeout)
		defer cancel()

		ticker := time.NewTicker(workspaceReadyPollInterval)
		defer ticker.Stop()

		checkReady := func() bool {
			if !delivered {
				code := r.actorRequest(pollCtx, http.MethodPost, host, port, runnerAssignmentPath, assignment, atespace, actorName)
				delivered = code == http.StatusOK
				if !delivered {
					return false
				}
				slog.Info("delivered task assignment to runner", "actor", actorName)
			}
			return workspaceReady || r.actorRequest(pollCtx, http.MethodGet, host, port, readyPath, nil, atespace, actorName) == http.StatusOK
		}

		// Initial check
		ready := checkReady()

		for !ready {
			select {
			case <-pollCtx.Done():
				goto DonePolling
			case <-ticker.C:
				ready = checkReady()
			}
		}
		workspaceReady = true
	}
DonePolling:
	if !delivered && workerIP != "" {
		// A cold start (image and runner volume pulls) can outlast the resume
		// call; keep delivering in the background until the runner takes it.
		slog.Info("task assignment not delivered yet; retrying in the background", "actor", actorName)
		go r.deliverInBackground(host, port, assignment, atespace, actorName)
	}

	// The server derives Ready from the actor's state and WorkspaceReady.
	if workspaceReady {
		if !conditionTrue(task, v1alpha1.ConditionWorkspaceReady) {
			setCondition(task, v1alpha1.ConditionWorkspaceReady, "True", v1alpha1.ReasonSetupComplete, fmt.Sprintf("Workspace setup completed at %s", workerIP), time.Now())
		}
	} else {
		setCondition(task, v1alpha1.ConditionWorkspaceReady, "False", v1alpha1.ReasonInitializing, fmt.Sprintf("Workspace is initializing at %s", workerIP), time.Now())
	}

	slog.Info("task successfully resumed",
		"name", task.Metadata.Name,
		"actor", actorName,
		"workerIP", workerIP,
		"workspaceReady", workspaceReady,
	)

	return nil
}

// setNotReady marks the task's Ready condition False. WorkspaceReady is left untouched:
// workspace setup is a one-time step whose result outlives actor failures and suspends.
func (r *TaskReconciler) setNotReady(task *v1alpha1.Task, reason, message string, t time.Time) {
	setCondition(task, v1alpha1.ConditionReady, "False", reason, message, t)
}

// conditionTrue reports whether the task currently has the given condition with status True.
func conditionTrue(task *v1alpha1.Task, condType string) bool {
	if task.Status == nil {
		return false
	}
	for _, c := range task.Status.Conditions {
		if c.Type == condType {
			return c.Status == "True"
		}
	}
	return false
}

// substrateErrorMessage returns the message to show users for a failed Substrate call.
// Substrate errors reach the reconciler wrapped (fmt.Errorf("...: %w", grpcErr)), and
// status.Convert on a wrapped error yields the full err.Error() including the
// "rpc error: code = ... desc = " boilerplate. Unwrapping to the underlying gRPC
// status recovers just the description Substrate wrote, e.g.
// `actor_template.containers[0].image: Invalid value: "...": must be pinned by digest`.
// Non-gRPC errors are returned verbatim.
func substrateErrorMessage(err error) string {
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		if s := gs.GRPCStatus(); s != nil && s.Message() != "" {
			return s.Message()
		}
	}
	return err.Error()
}

func setCondition(task *v1alpha1.Task, condType, status, reason, message string, t time.Time) {
	ts := timestamppb.New(t)
	for i, c := range task.Status.Conditions {
		if c.Type == condType {
			task.Status.Conditions[i].Status = status
			task.Status.Conditions[i].Reason = reason
			task.Status.Conditions[i].Message = message
			task.Status.Conditions[i].UpdateTime = ts
			return
		}
	}
	task.Status.Conditions = append(task.Status.Conditions, &v1alpha1.Condition{
		Type:       condType,
		Status:     status,
		Reason:     reason,
		Message:    message,
		UpdateTime: ts,
	})
}

// imageTemplateName derives the shared ActorTemplate name from the image, the
// runner volume, and the resource limits only. Per-task data (prompt, config,
// env) never reaches the template, so every task that runs the same image with
// the same limits shares one template and its warm snapshots. Bumping
// v1alpha1.RunnerVolumeImage rolls every image onto a new template.
func imageTemplateName(image string, resources *v1alpha1.ResourcesSpec) string {
	h := sha256.New()
	h.Write([]byte(image))
	h.Write([]byte{0})
	h.Write([]byte(v1alpha1.RunnerVolumeImage))
	h.Write([]byte{0})
	if b, err := proto.Marshal(resources); err == nil {
		h.Write(b)
	}
	return fmt.Sprintf("%s%x", sharedTemplatePrefix, h.Sum(nil)[:templateDigestBytes])
}

// sharedTemplatePrefix starts every name imageTemplateName produces.
const sharedTemplatePrefix = "ax-img-tmpl-"

// taskTemplatePattern matches every per-task ActorTemplate name earlier
// versions of AX produced for the given task, so they are still cleaned up.
// Shared image templates are never deleted with a task.
func taskTemplatePattern(taskName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf("^%s-tmpl-[0-9a-f]{%d}$", regexp.QuoteMeta(taskName), 2*templateDigestBytes))
}

// Delete cleans up Substrate resources when a Task is deleted: the actor,
// which shares the task's name, then every ActorTemplate provisioned for the task.
func (r *TaskReconciler) Delete(ctx context.Context, atespace, taskName string) error {
	if atespace == "" {
		atespace = "default"
	}
	slog.Info("deleting Substrate actor for task", "atespace", atespace, "task", taskName)
	if err := r.client.DeleteActor(ctx, atespace, taskName); err != nil {
		return err
	}
	if err := r.deleteTaskTemplates(ctx, atespace, taskName); err != nil {
		slog.Warn("could not clean up actor templates for task", "task", taskName, "error", err)
	}
	return nil
}

// deleteTaskTemplates removes all ActorTemplates belonging to the task. Templates
// accumulate across spec revisions, so this matches by name pattern rather than
// recomputing a single digest. If an actor deletion is still finishing in Substrate,
// template deletion may briefly return Aborted, so we retry with backoff.
func (r *TaskReconciler) deleteTaskTemplates(ctx context.Context, atespace, taskName string) error {
	templates, err := r.client.ListActorTemplates(ctx, atespace)
	if err != nil {
		return err
	}
	pattern := taskTemplatePattern(taskName)
	var errs []error
	for _, tmpl := range templates {
		name := tmpl.GetMetadata().GetName()
		if !pattern.MatchString(name) {
			continue
		}
		slog.Info("deleting Substrate actor template for task", "atespace", atespace, "task", taskName, "template", name)
		var delErr error
		for attempt := 0; attempt < 5; attempt++ {
			delErr = r.client.DeleteActorTemplate(ctx, atespace, name)
			if delErr == nil || status.Code(delErr) == codes.NotFound {
				delErr = nil
				break
			}
			select {
			case <-ctx.Done():
				delErr = ctx.Err()
				break
			case <-time.After(500 * time.Millisecond):
			}
		}
		if delErr != nil {
			errs = append(errs, delErr)
		}
	}
	return errors.Join(errs...)
}

// runnerAssignmentPath is the runner endpoint, on its probe port, that takes
// the task assignment. It is served by internal/metadata.
const runnerAssignmentPath = "/v1alpha1/assignment"

// encodeAssignment encodes the launch configuration of task (without status)
// and its bound workspace, which may be nil, as the runner's assignment.
func encodeAssignment(task *v1alpha1.Task, ws *v1alpha1.Workspace) ([]byte, error) {
	launchTask := proto.Clone(task).(*v1alpha1.Task)
	launchTask.Status = nil
	var a v1alpha1.Assignment
	taskYAML, err := yaml.Marshal(launchTask)
	if err != nil {
		return nil, fmt.Errorf("encoding task: %w", err)
	}
	a.TaskYAML = string(taskYAML)
	if ws != nil {
		wsYAML, err := yaml.Marshal(ws)
		if err != nil {
			return nil, fmt.Errorf("encoding workspace: %w", err)
		}
		a.WorkspaceYAML = string(wsYAML)
	}
	return json.Marshal(a)
}

// actorRequest sends a request to the runner in the task's actor, first
// directly at host:port and then through the atenet-router when
// ATENET_ROUTER_ADDR is set. It returns the first 200 status, or the last
// status seen (0 when no request got a response).
func (r *TaskReconciler) actorRequest(ctx context.Context, method, host, port, path string, body []byte, atespace, actorName string) int {
	urls := []string{"http://" + net.JoinHostPort(host, port) + path}
	if routerAddr := os.Getenv("ATENET_ROUTER_ADDR"); routerAddr != "" {
		routerURL := routerAddr
		if !strings.HasPrefix(routerURL, "http://") && !strings.HasPrefix(routerURL, "https://") {
			if strings.HasSuffix(routerURL, ":443") {
				routerURL = "https://" + routerURL
			} else {
				routerURL = "http://" + routerURL
			}
		}
		urls = append(urls, strings.TrimRight(routerURL, "/")+path)
	}
	last := 0
	for i, u := range urls {
		req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
		if err != nil {
			continue
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if i > 0 {
			req.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", atespace, actorName))
		}
		resp, err := r.httpClient.Do(req)
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return http.StatusOK
		}
		last = resp.StatusCode
	}
	return last
}

// assignmentRetryWindow bounds background delivery of a task assignment.
const assignmentRetryWindow = 15 * time.Minute

// deliverInBackground retries the assignment until the runner accepts it or
// assignmentRetryWindow passes. The runner treats repeats as no-ops, so a
// concurrent delivery from a later resume is harmless.
func (r *TaskReconciler) deliverInBackground(host, port string, assignment []byte, atespace, actorName string) {
	ctx, cancel := context.WithTimeout(context.Background(), assignmentRetryWindow)
	defer cancel()
	ticker := time.NewTicker(2 * workspaceReadyPollInterval)
	defer ticker.Stop()
	for {
		if r.actorRequest(ctx, http.MethodPost, host, port, runnerAssignmentPath, assignment, atespace, actorName) == http.StatusOK {
			slog.Info("delivered task assignment to runner", "actor", actorName)
			return
		}
		select {
		case <-ctx.Done():
			slog.Warn("gave up delivering task assignment to runner; it is retried on the next resume", "actor", actorName)
			return
		case <-ticker.C:
		}
	}
}
