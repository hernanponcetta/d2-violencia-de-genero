# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This is a boilerplate pipeline 'pdf_scrap'
generated using Kedro 0.16.2
"""

from kedro.pipeline import Pipeline, node
from .nodes import (
    tidy_data,
    buscar_violencia_genero,
    buscar_violencia_fisica,
    buscar_info_denunciante,
    buscar_genero_den,
    buscar_nacionalidad_denunciante,
    buscar_est_civil_denunciente,
    buscar_edad_denunciante,
    buscar_est_denunciante,
    buscar_domic_denunciante,
    buscar_villa_denunciante,
    buscar_ocupac_denunciante,
    buscar_info_acusado,
    buscar_genero_acusado,
    buscar_nacionalidad_acusado,
    buscar_est_civil_acusado,
    buscar_edad_acusado,
    to_excel,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(func=tidy_data, inputs="filt_list", outputs="data",),
            node(
                func=buscar_violencia_genero,
                inputs="data",
                outputs="violencia_de_genero",
            ),
            node(
                func=buscar_violencia_fisica, inputs="data", outputs="violencia_fisica",
            ),
            node(
                func=buscar_info_denunciante, inputs="data", outputs="info_denunciante",
            ),
            node(func=buscar_info_acusado, inputs="data", outputs="info_acusado",),
            node(
                func=buscar_genero_den,
                inputs="info_denunciante",
                outputs="genero_denunciante",
            ),
            node(
                func=buscar_nacionalidad_denunciante,
                inputs="info_denunciante",
                outputs="nacionalidad_denunciante",
            ),
            node(
                func=buscar_est_civil_denunciente,
                inputs="info_denunciante",
                outputs="est_civil_denunciante",
            ),
            node(
                func=buscar_edad_denunciante,
                inputs="info_denunciante",
                outputs="edad_denunciante",
            ),
            node(
                func=buscar_est_denunciante,
                inputs="info_denunciante",
                outputs="est_denunciante",
            ),
            node(
                func=buscar_domic_denunciante,
                inputs="info_denunciante",
                outputs="domic_denunciante",
            ),
            node(
                func=buscar_villa_denunciante,
                inputs="info_denunciante",
                outputs="villa_denunciante",
            ),
            node(
                func=buscar_ocupac_denunciante,
                inputs="info_denunciante",
                outputs="ocupac_denunciante",
            ),
            node(
                func=buscar_genero_acusado,
                inputs="info_acusado",
                outputs="genero_acusado",
            ),
            node(
                func=buscar_nacionalidad_acusado,
                inputs="info_acusado",
                outputs="nacionalida_acusado",
            ),
            node(
                func=buscar_est_civil_acusado,
                inputs="info_acusado",
                outputs="est_civil_acusado",
            ),
            node(
                func=buscar_edad_acusado,
                inputs="info_acusado",
                outputs="edad_acusado",
            ),
            node(
                func=to_excel,
                inputs=[
                    "violencia_de_genero",
                    "violencia_fisica",
                    "genero_denunciante",
                    "nacionalidad_denunciante",
                    "est_civil_denunciante",
                    "edad_denunciante",
                    "est_denunciante",
                    "domic_denunciante",
                    "villa_denunciante",
                    "ocupac_denunciante",
                    "genero_acusado",
                    "nacionalida_acusado",
                    "est_civil_acusado",
                    "edad_acusado"
                ],
                outputs="excel_ovd_data",
            ),
        ]
    )
