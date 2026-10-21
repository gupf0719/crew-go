package common

type ImageUrlContentBlock struct {
	Type     ContentBlockType `json:"type"`
	ImageUrl *ImageUrl        `json:"imageUrl"`
}
