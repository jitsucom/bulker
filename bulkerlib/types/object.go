package types

import "github.com/jitsucom/bulker/jitsubase/utils"

// TODO: move type conversion here
type Object map[string]any

func (o Object) Id() any {
	return utils.MapNVLKeys(o, "message_id", "messageId", "id")
}
