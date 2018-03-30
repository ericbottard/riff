/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package v1

import (
	. "github.com/onsi/gomega"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("The mediatype library", func() {

	It("should parse the empty string to empty map", func() {
		a := ParseAccept("")
		Expect(a).To(BeEmpty())
	})
	It("should parse assuming q=1", func() {
		a := ParseAccept("text/plain")
		Expect(a).To(HaveKeyWithValue(BeEquivalentTo("text/plain"), BeEquivalentTo( "1.0")))
	})
	It("should parse multiple values", func() {
		a := ParseAccept("text/plain,text/xml;q=0.4")
		Expect(a).To(HaveKeyWithValue(BeEquivalentTo("text/plain"), BeEquivalentTo( "1.0")))
		Expect(a).To(HaveKeyWithValue(BeEquivalentTo("text/xml"), BeEquivalentTo( "0.4")))
	})

	It("should parse multiple values", func() {
		a1 := ParseAccept("text/plain,text/xml;q=0.4,*/*;q=0.5")
		a2 := ParseAccept("application/json,text/*;q=0.6")

		expected := ParseAccept("text/plain;q=0.6, text/xml;q=0.4, application/json; q=0.5 ,  text/*; q=0.5")
		Expect(a1.Intersect(a2)).To(Equal(expected))

		Expect(a1.Intersect(a2)).To(Equal(a2.Intersect(a1)))

		Expect(a1.Intersect(a1)).To(Equal(a1))
	})


})