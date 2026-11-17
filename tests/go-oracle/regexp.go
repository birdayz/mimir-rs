// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"encoding/json"

	"github.com/prometheus/prometheus/model/labels"
)

type regexMatcherErrorsRequest struct {
	Patterns []string `json:"patterns"`
}

type regexMatcherErrorsResponse struct {
	// Errors[i] is the error of labels.NewMatcher(MatchRegexp, "x", Patterns[i]), "" if valid.
	Errors []string `json:"errors"`
}

// cmdRegexMatcherErrors reports the Prometheus regex matcher compile errors
// (labels.NewFastRegexMatcher -> regexp/syntax.Parse) for each pattern.
func cmdRegexMatcherErrors(input []byte) (any, error) {
	var req regexMatcherErrorsRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, err
	}
	resp := regexMatcherErrorsResponse{Errors: make([]string, len(req.Patterns))}
	for i, p := range req.Patterns {
		if _, err := labels.NewMatcher(labels.MatchRegexp, "x", p); err != nil {
			resp.Errors[i] = err.Error()
		}
	}
	return resp, nil
}
