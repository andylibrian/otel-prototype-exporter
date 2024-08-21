package natsexporter

// Config defines configuration for file exporter.
type Config struct {

	// Path of the file to write to. Path is relative to current directory.
	Path string `mapstructure:"path"`
}
