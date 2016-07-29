package main

import (
	"errors"
	"fmt"
	"github.com/tarantool/go-tarantool"
	"gopkg.in/vmihailenco/msgpack.v2"
	"math/rand"
	"reflect"
	"time"
)

const (
	HEX_LETTERS_DIGITS    = "abcdef0123456789"
	PUSH_TECH_GCM         = "g"
	PUSH_TECH_FCM         = "f"
	AUTH_STRING_LEN       = 16
)

// Have to match push_db_server.lua
type ResultCode int

const (
	RES_OK ResultCode = 0

	RES_ERR_UNKNOWN_DEV_ID            ResultCode = -1
	RES_ERR_UNKNOWN_SUB_ID            ResultCode = -2
	RES_ERR_MISMATCHING_SUB_ID_DEV_ID ResultCode = -3

	RES_ERR_DATABASE ResultCode = -100
)

func (r *ResultCode) String() string {
	switch *r {
	case RES_OK:
		return "ok"
	case RES_ERR_UNKNOWN_DEV_ID:
		return "errUnknownDeviceId"
	case RES_ERR_UNKNOWN_SUB_ID:
		return "errUnknownSubId"
	case RES_ERR_MISMATCHING_SUB_ID_DEV_ID:
		return "errMismatchingSubIdDevId"
	case RES_ERR_DATABASE:
		return "errDatabase"
	default:
		return fmt.Sprintf("Unknown: %d", *r)
	}
}

/* ----- */

func genRandomString(keylen int) string {
	l := len(HEX_LETTERS_DIGITS)
	b := make([]byte, keylen)
	for i := 0; i < keylen; i++ {
		b[i] = HEX_LETTERS_DIGITS[rand.Intn(l)]
	}
	return string(b)
}

func genPushToken() string {
	l := len(HEX_LETTERS_DIGITS)
	b := make([]byte, 160)
	for i := 0; i < 40; i++ {
		v := HEX_LETTERS_DIGITS[rand.Intn(l)]
		b[i] = v
		b[i+40] = v
		b[i+80] = v
		b[i+120] = v
	}
	return string(b)
}

/* ----- */

type Millitime int64

const (
	TIME_MS_SECOND     Millitime = 1000
	TIME_MS_5_SECONDS            = TIME_MS_SECOND * 5
	TIME_MS_15_SECONDS           = TIME_MS_SECOND * 15
	TIME_MS_5_MINUTE             = TIME_MS_SECOND * 60
)

func milliTime() Millitime {
	return Millitime(time.Now().UnixNano() / int64(time.Millisecond))
}

func milliTimeToTime(t Millitime) time.Time {
	return time.Unix(int64(t)/1000, (int64(t)%1000)*1000000)
}

func milliTimeToMillis(t Millitime) int64 {
	return int64(t)
}

func encodeMilliTime(e *msgpack.Encoder, t Millitime) error {
	return e.EncodeInt64(int64(t))
}

func decodeMilliTime(d *msgpack.Decoder) (t Millitime, err error) {
	i, err := d.DecodeInt64()
	t = Millitime(i)
	return
}

/* ----- */

type DevEnt struct {
	dev_id             string
	auth               string
	push_token         string
	push_tech          string
	ping_ts            Millitime
	change_ts          Millitime
	change_count int
}

func (dev DevEnt) String() string {
	return fmt.Sprintf("[dev_id = %q, auth = %q, push_token = %q, push_tech = %q, ping_ts = %s, change_ts = %s]",
		dev.dev_id, dev.auth, dev.push_token, dev.push_tech,
		milliTimeToTime(dev.ping_ts), milliTimeToTime(dev.change_ts))
}

func encodeDevEnt(e *msgpack.Encoder, v reflect.Value) error {
	m := v.Interface().(DevEnt)
	if err := e.EncodeSliceLen(7); err != nil {
		return err
	}
	if err := e.EncodeString(m.dev_id); err != nil {
		return err
	}
	if err := e.EncodeString(m.auth); err != nil {
		return err
	}
	if err := e.EncodeString(m.push_token); err != nil {
		return err
	}
	if err := e.EncodeString(m.push_tech); err != nil {
		return err
	}
	if err := encodeMilliTime(e, m.ping_ts); err != nil {
		return err
	}
	if err := encodeMilliTime(e, m.change_ts); err != nil {
		return err
	}
	if err := e.EncodeInt(m.change_count); err != nil {
		return err
	}
	return nil
}

func decodeDevEnt(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	m := v.Addr().Interface().(*DevEnt)
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 7 {
		return fmt.Errorf("decodeDevEnt array len doesn't match: %d", l)
	}
	if m.dev_id, err = d.DecodeString(); err != nil {
		return err
	}
	if m.auth, err = d.DecodeString(); err != nil {
		return err
	}
	if m.push_token, err = d.DecodeString(); err != nil {
		return err
	}
	if m.push_tech, err = d.DecodeString(); err != nil {
		return err
	}
	if m.ping_ts, err = decodeMilliTime(d); err != nil {
		return err
	}
	if m.change_ts, err = decodeMilliTime(d); err != nil {
		return err
	}
	if m.change_count, err = d.DecodeInt(); err != nil {
		return err
	}
	return nil
}

type SubEnt struct {
	sub_id    string
	dev_id    string
	ping_ts   Millitime
	change_ts Millitime
}

func (sub SubEnt) String() string {
	return fmt.Sprintf("[sub_id = %q, dev_id = %q, ping_ts = %s, change_ts = %s]",
		sub.sub_id, sub.dev_id,
		milliTimeToTime(sub.ping_ts),
		milliTimeToTime(sub.change_ts))
}

func encodeSubEnt(e *msgpack.Encoder, v reflect.Value) error {
	m := v.Interface().(SubEnt)
	if err := e.EncodeSliceLen(4); err != nil {
		return err
	}
	if err := e.EncodeString(m.sub_id); err != nil {
		return err
	}
	if err := e.EncodeString(m.dev_id); err != nil {
		return err
	}
	if err := encodeMilliTime(e, m.ping_ts); err != nil {
		return err
	}
	if err := encodeMilliTime(e, m.change_ts); err != nil {
		return err
	}
	return nil
}

func decodeSubEnt(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	m := v.Addr().Interface().(*SubEnt)
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 4 {
		return fmt.Errorf("decodeSubEnt array len doesn't match: %d", l)
	}
	if m.sub_id, err = d.DecodeString(); err != nil {
		return err
	}
	if m.dev_id, err = d.DecodeString(); err != nil {
		return err
	}
	if m.ping_ts, err = decodeMilliTime(d); err != nil {
		return err
	}
	if m.change_ts, err = decodeMilliTime(d); err != nil {
		return err
	}
	return nil
}

type ResultEnt struct {
	code ResultCode
	s    string
}

func (res ResultEnt) String() string {
	return fmt.Sprintf("[code = %s, s = %q]",
		&res.code, res.s)
}

func encodeResultEnt(e *msgpack.Encoder, v reflect.Value) error {
	m := v.Interface().(ResultEnt)
	if err := e.EncodeSliceLen(2); err != nil {
		return err
	}
	if err := e.EncodeInt(int(m.code)); err != nil {
		return err
	}
	if err := e.EncodeString(m.s); err != nil {
		return err
	}
	return nil
}

func decodeResultEnt(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	m := v.Addr().Interface().(*ResultEnt)
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l < 1 || l > 2 {
		return fmt.Errorf("decodeResultEnt array len doesn't match: %d", l)
	}
	if code, err := d.DecodeInt(); err != nil {
		return err
	} else {
		m.code = ResultCode(code)
	}
	if l >= 2 {
		if m.s, err = d.DecodeString(); err != nil {
			return err
		}
	} else {
		m.s = ""
	}
	return nil
}

func init() {
	msgpack.Register(reflect.TypeOf(DevEnt{}), encodeDevEnt, decodeDevEnt)
	msgpack.Register(reflect.TypeOf(SubEnt{}), encodeSubEnt, decodeSubEnt)
	msgpack.Register(reflect.TypeOf(ResultEnt{}), encodeResultEnt, decodeResultEnt)
}

/* ----- */

type PushDbModel struct {
	dbconn *tarantool.Connection

	schema             *tarantool.Schema
	space_devs         *tarantool.Space
	index_devs_primary *tarantool.Index
}

func NewPushDbModel(dbconn *tarantool.Connection) *PushDbModel {
	model := &PushDbModel{dbconn: dbconn}

	model.schema = dbconn.Schema

	model.space_devs = model.schema.Spaces["devs"]
	model.index_devs_primary = model.space_devs.Indexes["primary"]

	return model
}

func (model *PushDbModel) getDevEnt(dev_id string) (*DevEnt, error) {
	var res []DevEnt
	err := model.dbconn.SelectTyped("devs", "primary", 0, 1, tarantool.IterEq, []interface{}{dev_id}, &res)
	if err != nil {
		s := fmt.Sprintf("Error calling device select: %s", err)
		return nil, errors.New(s)
	}

	if res == nil {
		s := "Error calling device result set"
		return nil, errors.New(s)
	}

	if len(res) == 0 {
		return nil, nil
	}

	return &res[0], nil
}

func (model *PushDbModel) doCreateDev(dev_id string, auth string, push_token string, push_tech string, now Millitime) (*DevEnt, ResultCode, error) {
	var fname = "push_CreateDev"

	var res []DevEnt
	err := model.dbconn.CallTyped(fname, []interface{}{dev_id, auth, push_token, push_tech, now}, &res)
	if err != nil {
		s := fmt.Sprintf("Error calling %s: %s", fname, err.Error())
		return nil, RES_ERR_DATABASE, errors.New(s)
	}

	if res == nil || len(res) != 1 {
		s := fmt.Sprintf("Error calling %s: result set", fname)
		return nil, RES_ERR_DATABASE, errors.New(s)
	}

	return &res[0], RES_OK, nil
}

func (model *PushDbModel) doCreateSub(dev_id string, sub_id string, now Millitime) (ResultCode, error) {
	var fname = "push_CreateSub"

	var res []ResultEnt
	err := model.dbconn.CallTyped(fname, []interface{}{dev_id, sub_id, now}, &res)
	if err != nil {
		s := fmt.Sprintf("Error calling %s: %s", fname, err.Error())
		return RES_ERR_DATABASE, errors.New(s)
	}
	if res == nil || len(res) != 1 {
		s := fmt.Sprintf("Error calling %s: result set", fname)
		return RES_ERR_DATABASE, errors.New(s)
	}

	return res[0].code, nil
}


func (model *PushDbModel) doPingChangeSub(dev_id string, sub_id string, change bool, set_ping_change_ts Millitime) (ResultCode, error) {
	var fname = "push_PingSub"
	if change {
		fname = "push_ChangeSub"
	}

	var res []ResultEnt

	err := model.dbconn.CallTyped(fname, []interface{}{dev_id, sub_id, set_ping_change_ts}, &res)
	if err != nil {
		s := fmt.Sprintf("Error calling %s: %s", fname, err.Error())
		return RES_ERR_DATABASE, errors.New(s)
	}
	if res == nil || len(res) != 1 {
		s := fmt.Sprintf("Error calling %s: result set", fname)
		return RES_ERR_DATABASE, errors.New(s)
	}

	return res[0].code, nil
}
