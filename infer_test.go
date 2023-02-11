package avro

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type E struct {
	F string
}

type A struct {
	B string `avro:"b"`
	C int
	E E
}

func TestInferSchema(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "simple nested struct",
			args: args{
				v: A{
					B: "hello",
					C: 1,
					E: E{
						F: "world",
					},
				},
			},
			want:    `{"name":"A","type":"record","fields":[{"name":"b","type":"string"},{"name":"C","type":"int"},{"name":"E","type":"record","fields":[{"name":"F","type":"string"}]}]}`,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InferSchema(tt.args.v)
			if !tt.wantErr(t, err, fmt.Sprintf("InferSchema(%v)", tt.args.v)) {
				return
			}

			t.Log(got)

			assert.Equalf(t, tt.want, got, "InferSchema(%v)", tt.args.v)
		})
	}
}
