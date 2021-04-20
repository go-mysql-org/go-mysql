package canal

import (
	"io"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

func init() {
	ast.NewValueExpr = newValueExpr
	ast.NewParamMarkerExpr = newParamExpr
	ast.NewDecimal = func(_ string) (interface{}, error) {
		return nil, nil
	}
	ast.NewHexLiteral = func(_ string) (interface{}, error) {
		return nil, nil
	}
	ast.NewBitLiteral = func(_ string) (interface{}, error) {
		return nil, nil
	}
}

type paramExpr struct {
	valueExpr
}

func newParamExpr(_ int) ast.ParamMarkerExpr {
	return &paramExpr{}
}
func (pe *paramExpr) SetOrder(o int) {}

type valueExpr struct {
	ast.TexprNode
}

func newValueExpr(_ interface{}, _ string, _ string) ast.ValueExpr  { return &valueExpr{} }
func (ve *valueExpr) SetValue(val interface{})                      {}
func (ve *valueExpr) GetValue() interface{}                         { return nil }
func (ve *valueExpr) GetDatumString() string                        { return "" }
func (ve *valueExpr) GetString() string                             { return "" }
func (ve *valueExpr) GetProjectionOffset() int                      { return 0 }
func (ve *valueExpr) SetProjectionOffset(offset int)                {}
func (ve *valueExpr) Restore(ctx *format.RestoreCtx) error          { return nil }
func (ve *valueExpr) Accept(v ast.Visitor) (node ast.Node, ok bool) { return }
func (ve *valueExpr) Text() string                                  { return "" }
func (ve *valueExpr) SetText(text string)                           {}
func (ve *valueExpr) Format(w io.Writer)                            {}
