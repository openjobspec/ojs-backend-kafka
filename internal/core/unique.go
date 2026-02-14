package core

// UniquePolicy defines deduplication rules for a job.
type UniquePolicy struct {
	Keys       []string `json:"keys"`
	Period     string   `json:"period,omitempty"`
	OnConflict string   `json:"on_conflict,omitempty"`
	States     []string `json:"states,omitempty"`
}
