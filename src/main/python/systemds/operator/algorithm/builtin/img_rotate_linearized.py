# -------------------------------------------------------------
#
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
#
# -------------------------------------------------------------

# Autogenerated By   : src/main/python/generator/generator.py
# Autogenerated From : scripts/builtin/img_rotate_linearized.dml

from typing import Dict, Iterable

from systemds.operator import OperationNode, Matrix, Frame, List, MultiReturn, Scalar
from systemds.utils.consts import VALID_INPUT_TYPES


def img_rotate_linearized(img_in: Matrix,
                          radians: float,
                          fill_value: float,
                          s_cols: int,
                          s_rows: int):
    """
     The Linearized Image Rotate function rotates the linearized input images counter-clockwise around the center.
     Uses nearest neighbor sampling.
    
    
    
    :param img_in: Linearized input images as 2D matrix with top left corner at [1, 1]
    :param radians: The value by which to rotate in radian.
    :param fill_value: The background color revealed by the rotation
    :return: Output images in linearized form as 2D matrix with top left corner at [1, 1]
    """

    params_dict = {'img_in': img_in, 'radians': radians, 'fill_value': fill_value, 's_cols': s_cols, 's_rows': s_rows}
    return Matrix(img_in.sds_context,
        'img_rotate_linearized',
        named_input_nodes=params_dict)
