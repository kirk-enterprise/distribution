package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/api/v2"
	"github.com/gorilla/handlers"
)

// tagsDispatcher constructs the tags handler api endpoint.
func tagsDispatcher(ctx *Context, r *http.Request) http.Handler {
	tagsHandler := &tagsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		"GET": http.HandlerFunc(tagsHandler.GetTags),
	}
}

// tagDispatcher constructs the tag delete handler
func tagDispatcher(ctx *Context, r *http.Request) http.Handler {
	tagHandler := &tagHandler{
		Context: ctx,
		Tag:  getTag(ctx),
	}

	thandler := handlers.MethodHandler{
	}

	if !ctx.readOnly {
		thandler["DELETE"] = http.HandlerFunc(tagHandler.DeleteTag)
	}

	return thandler
}

// tagsHandler handles requests for lists of tags under a repository name.
type tagsHandler struct {
	*Context
}

type tagHandler struct {
	*Context
	Tag string
}

type tagsAPIResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// GetTags returns a json list of tags for a specific image name.
func (th *tagsHandler) GetTags(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	tagService := th.Repository.Tags(th)
	tags, err := tagService.All(th)
	if err != nil {
		switch err := err.(type) {
		case distribution.ErrRepositoryUnknown:
			th.Errors = append(th.Errors, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"name": th.Repository.Named().Name()}))
		case errcode.Error:
			th.Errors = append(th.Errors, err)
		default:
			th.Errors = append(th.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		}
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	enc := json.NewEncoder(w)
	if err := enc.Encode(tagsAPIResponse{
		Name: th.Repository.Named().Name(),
		Tags: tags,
	}); err != nil {
		th.Errors = append(th.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}

// DeleteTag only do the untag and leave the manifest untouched
func (th *tagHandler) DeleteTag(w http.ResponseWriter, r *http.Request) {
	tagService := th.Repository.Tags(th)
	if err := tagService.Untag(th.Context, th.Tag); err != nil {
		th.Errors = append(th.Errors, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
