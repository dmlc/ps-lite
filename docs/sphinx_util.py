# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import sys, os, subprocess


if not os.path.exists('../recommonmark'):
    subprocess.call('cd ..; git clone https://github.com/tqchen/recommonmark', shell = True)
else:
    subprocess.call('cd ../recommonmark; git pull', shell=True)

sys.path.insert(0, os.path.abspath('../recommonmark/'))

from recommonmark import parser, transform
MarkdownParser = parser.CommonMarkParser
AutoStructify = transform.AutoStructify

# MarkdownParser.github_doc_root = github_doc_root

def generate_doxygen_xml(app):
    """Run the doxygen make commands"""
    subprocess.call('doxygen')
