/*
 * Copyright 2018 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package cmd

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/projectriff/riff/riff-cli/pkg/osutils"
	"github.com/spf13/cobra"
)

func TestUpdateCommandImplicitPath(t *testing.T) {
	rootCmd, _ , applyOptions := setupUpdateTest()
	as := assert.New(t)
	rootCmd.SetArgs([]string{"update", "--dry-run", osutils.Path("../test_data/shell/echo")})

	_, err := rootCmd.ExecuteC()
	as.NoError(err)
	as.Equal("../test_data/shell/echo", applyOptions.FilePath)
}

func TestUpdateCommandExplicitPath(t *testing.T) {
	rootCmd, _ , applyOptions := setupUpdateTest()
	as := assert.New(t)
	rootCmd.SetArgs([]string{"update", "--dry-run", "-f", osutils.Path("../test_data/shell/echo")})

	_, err := rootCmd.ExecuteC()
	as.NoError(err)
	as.Equal("../test_data/shell/echo", applyOptions.FilePath)
}

func setupUpdateTest() (*cobra.Command, *BuildOptions, *ApplyOptions) {
	root := Root()
	buildCmd, buildOptions := Build()

	applyCmd, applyOptions := Apply()

	update := Update(buildCmd, applyCmd)
	root.AddCommand(update)
	return rootCmd, buildOptions, applyOptions
}
