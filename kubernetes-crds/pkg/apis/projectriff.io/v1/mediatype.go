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
	"strings"
	"strconv"
)

type MediaType string
type Quality string

type AcceptedMediaTypes map[MediaType]Quality

func (s1 AcceptedMediaTypes) Intersect(s2 AcceptedMediaTypes) AcceptedMediaTypes {
	result := make(AcceptedMediaTypes, len(s1))
	for t1, q1 := range s1 {
		q := min(q1, s2[t1])
		if q == "" {
			q = min(q1, s2[wildcardFor(t1)])
		}
		if q == "" {
			q = min(q1, s2["*/*"])
		}
		if q != "" {
			result[t1] = q
		}
	}


	for t2, q2 := range s2 {
		q := min(q2, s1[t2])
		if q == "" {
			q = min(q2, s1[wildcardFor(t2)])
		}
		if q == "" {
			q = min(q2, s1["*/*"])
		}
		if q != "" {
			result[t2] = q
		}
	}
	return result
}

func wildcardFor(mediaType MediaType) MediaType {
	if mediaType[len(mediaType)-1] == '*' {
		return mediaType
	} else {
		slash := strings.IndexRune(string(mediaType), '/')
		return MediaType(mediaType[:slash+1] + "*")
	}
}

func min(q1 Quality, q2 Quality) Quality {
	if q1 == "" || q2 == "" {
		return ""
	}
	f1, _ := strconv.ParseFloat(string(q1), 32)
	f2, _ := strconv.ParseFloat(string(q2), 32)
	if f1 < f2 {
		return q1
	} else {
		return q2
	}
}

func ParseAccept(accept string) AcceptedMediaTypes {
	result := make(AcceptedMediaTypes)
	if strings.TrimSpace(accept) == "" {
		return result
	}
	for  _, v := range strings.Split(accept, ",") {
		mt_q := strings.Split(v, ";")
		switch len(mt_q) {
		case 1:
			result[MediaType(strings.TrimSpace(mt_q[0]))] = "1.0"
		case 2:
			offset := strings.Index(mt_q[1], "q=") + len("q=")
			q := strings.TrimSpace(mt_q[1][offset:])
			result[MediaType(strings.TrimSpace(mt_q[0]))] = Quality(q)
		default:
			panic("Unparseable Accept value: " + accept)
		}
	}
	return result
}