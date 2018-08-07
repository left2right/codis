// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

type Product struct {
	Name string `json:"name"`
	Auth string `json:"auth"`
}

func (g *Product) Encode() []byte {
	return jsonEncode(g)
}
