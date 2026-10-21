package common

type FileContentBlock struct {
	Type ContentBlockType `json:"type"`
	File *FileData        `json:"file"`
}
