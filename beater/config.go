package beater

type SqConfig struct {
	Path       *string `config:"path"`
	Path_since *string `config:"path_since"`
	Batch      *int    `config:"batch"`
}
type ConfigSettings struct {
	Input SqConfig
}
