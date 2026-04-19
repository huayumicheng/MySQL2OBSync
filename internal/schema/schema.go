package schema

type TableSchema struct {
	TableName   string
	Columns     []ColumnSchema
	PrimaryKey  string
	PrimaryKeys []string
}

type ColumnSchema struct {
	Name         string
	ColumnType   string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	Extra        string
	Comment      string
}

type IndexSchema struct {
	Name     string
	IsUnique bool
	IsPrimary bool
	Columns  []string
}

type ConstraintSchema struct {
	Name       string
	Type       string
	Columns    []string
	RefTable   string
	RefColumns []string
}

type ExtendedTableSchema struct {
	TableSchema
	Indexes       []IndexSchema
	Constraints   []ConstraintSchema
	TableComment  string
}

