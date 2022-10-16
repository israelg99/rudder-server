package warehouseutils

const UUID_TS_COLUMN = "uuid_ts"

type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFomat(columnName string) string
	AddColumn(columnName, columnType string, val interface{})
	AddRow(columnNames, values []string)
	AddEmptyColumn(columnName string)
	WriteToString() (string, error)
	Write() error
}

func GetNewEventLoader(destinationType, loadFileType string, w LoadFileWriterI, c int) EventLoader {
	switch loadFileType {
	case LOAD_FILE_TYPE_JSON:
		return NewJSONLoader(destinationType, w)
	case LOAD_FILE_TYPE_PARQUET:
		return NewParquetLoader(destinationType, w, c)
	default:
		return NewCSVLoader(destinationType, w)
	}
}
