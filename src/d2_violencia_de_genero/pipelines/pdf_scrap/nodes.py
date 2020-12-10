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

import pandas as pd
import re
import string
import datetime as dt

barr_zona = dict.fromkeys(
    [
        "barracas",
        "boca",
        "la boca",
        "nueva pompeya",
        "parque patricios",
        "villa lugano",
        "lugano",
        "villa riachuelo",
        "villa soldati",
        "soldati",
        "liniers",
        "mataderos",
        "parque avellaneda",
    ],
    "zona_sur",
)
barr_zona.update(
    dict.fromkeys(
        [
            "coghlan",
            "saavedra",
            "villa urquiza",
            "belgrano",
            "colegiales",
            "nuñez",
            "palermo",
            "agronomia",
            "agronomía",
            "chacarita",
            "parque chas",
            "paternal",
            "villa crespo",
            "villa ortuzar",
            "villa ortúzar",
        ],
        "zona_norte",
    )
)
barr_zona.update(
    dict.fromkeys(
        [
            "constitucion",
            "constitución",
            "monserrat",
            "puerto madero",
            "retiro",
            "san nicolas",
            "san nicolás",
            "san telmo",
            "recoleta",
            "la recoleta",
            "balvanera",
            "san cristobal",
            "san cristóbal",
        ],
        "zona_este",
    )
)
barr_zona.update(
    dict.fromkeys(
        [
            "almagro",
            "boedo",
            "caballito",
            "flores",
            "parque chacabuco",
            "floresta",
            "monte castro",
            "montecastro",
            "velez sarsfield",
            "versalles",
            "villa luro",
            "villa real",
            "villa del parque",
            "villa devoto",
            "villa gral. mitre",
            "villa general mitre",
            "villa santa rita",
        ],
        "zona_oeste",
    )
)


def tidy_data(filt_list):
    data = " ".join(filt_list)
    data = data.lower()
    data = (
        data.replace("  ", " ")
        .replace("'", '"')
        .replace("´", '"')
        .replace("‘", '"')
        .replace("’", '"')
        .replace("primer día", "1 día")
    )
    return data


def buscar_violencia_genero(data):
    return (
        "buscar_manualmente"
        if re.search(r"violencia de género", data) == None
        else "si"
    )


def buscar_violencia_fisica(data):
    return (
        "buscar_manualmente"
        if re.search(
            r"preguntada acerca de si tiene (?:lesiones|marcas externas).*?:.*?s[ií]",
            data,
        )
        == None
        else "si"
    )


def buscar_info_denunciante(data):
    info_denunciante = re.search(
        r"comparece ante(.*?)seguidamente, se le hace saber", data
    )
    if info_denunciante == None:
        info_denunciante = "buscar_manualmente"
    else:
        info_denunciante = info_denunciante.groups(1)[0]
    return info_denunciante


def buscar_info_acusado(data):
    infoa_cusado = re.search(
        r"para exponer la situación planteada (.*?) manifiesta que", data
    ).groups(1)[0]
    return infoa_cusado


def buscar_genero_den(info_denunciante):
    findhObj = re.search(r"nación,\s(\S+)\s", info_denunciante)
    if findhObj is None:
        genero_denunciante = "buscar_manualmente"
    elif findhObj.groups(1)[0] == "el":
        genero_denunciante = "masculino"
    elif findhObj.groups(1)[0] == "la":
        genero_denunciante = "femenino"
    else:
        genero_denunciante = "buscar_manualmente"
    return genero_denunciante


def buscar_nacionalidad_denunciante(info_denunciante):
    nacionalidad_denunciante = re.search(r" de nacionalidad (.*?),", info_denunciante)
    if nacionalidad_denunciante == None:
        nacionalidad_denunciante = "buscar_manualmente"
    else:
        nacionalidad_denunciante = nacionalidad_denunciante.groups(1)[0]
    return nacionalidad_denunciante


def buscar_est_civil_denunciente(info_denunciante):
    est_civil_denunciante = re.search(r" de estado civil (.*?),", info_denunciante)
    if est_civil_denunciante == None:
        est_civil_denunciante = "buscar_manualmente"
    else:
        est_civil_denunciante = est_civil_denunciante.groups(1)[0]
    return est_civil_denunciante


def buscar_edad_denunciante(info_denunciante):
    edad_denunciante = re.search(r"de (\d+)\s+\w+\s+de edad", info_denunciante)
    if edad_denunciante == None:
        edad_denunciante = "buscar_manualmente"
    else:
        edad_denunciante = edad_denunciante.groups(1)[0]
    return edad_denunciante


def buscar_est_denunciante(info_denunciante):
    est_denunciante = re.search(r"(estudios .*?etos)", info_denunciante)
    if est_denunciante == None:
        est_denunciante = "buscar_manualmente"
    else:
        est_denunciante = est_denunciante.groups(1)[0]
    return est_denunciante


def buscar_domic_denunciante(info_denunciante):
    domic_denunciante = re.search(
        r"barrio de (\w+\s\w+).*?ciudad.*?,", info_denunciante
    )
    if domic_denunciante == None:
        domic_denunciante = "buscar_manualmente"
    else:
        domic_denunciante = (
            domic_denunciante.groups(1)[0].replace(" en", "").replace(" de", "")
        )
        domic_denunciante = [
            domic_denunciante.replace(key, value)
            for key, value in barr_zona.items()
            if domic_denunciante == key
        ]
    return domic_denunciante


def buscar_villa_denunciante(info_denunciante):
    villa_denunciante = re.search(r"([vV]illa)\s\d+.*?,", info_denunciante)
    if villa_denunciante == None:
        villa_denunciante = "buscar_manualmente"
    else:
        villa_denunciante = "si"
    return villa_denunciante


def buscar_ocupac_denunciante(info_denunciante):
    ocupac_denunciante = re.search(
        r"((?:se desempeña|condición laboral|de ocupación|emplead[oa]).*?)[,.]",
        info_denunciante,
    )
    if ocupac_denunciante == None:
        ocupac_denunciante = "buscar_manualmente"
    else:
        ocupac_denunciante = ocupac_denunciante.groups(1)[0]
    return ocupac_denunciante


def buscar_genero_acusado(info_acusado):
    porcion_genero_acusado = re.search(r"(.*?)de nacionalidad", info_acusado)
    if porcion_genero_acusado == None:
        porcion_genero_acusado = "buscar_manualmente"
    else:
        porcion_genero_acusado = porcion_genero_acusado.groups(1)[0]

    findhObj = re.search(r"(\S+)\ssr", porcion_genero_acusado)

    if findhObj is None:
        genero_acusado = "buscar_manualmente"
    if findhObj.groups(1)[0] == "el":
        genero_acusado = "masculino"
    elif findhObj.groups(1)[0] == "la":
        genero_acusado = "femenino"
    else:
        genero_acusado = "buscar_manualmente"

    return genero_acusado


def buscar_nacionalidad_acusado(info_acusado):
    nacionalidad_acusado = re.search(r" de nacionalidad (.*?),", info_acusado)
    if nacionalidad_acusado == None:
        nacionalidad_acusado = "buscar_manualmente"
    else:
        nacionalidad_acusado = nacionalidad_acusado.groups(1)[0]
    return nacionalidad_acusado

def buscar_est_civil_acusado(info_acusado):
    est_civil_acusado = re.search(r' de estado civil (.*?),', info_acusado)
    if est_civil_acusado == None:
        est_civil_acusado = "buscar_manualmente"
    else:
        est_civil_acusado = est_civil_acusado.groups(1)[0]
    return est_civil_acusado

def buscar_edad_acusado(info_acusado):
    edad_acusado = re.search(r'de (\d+)\s+\w+\s+de edad', info_acusado)
    if edad_acusado == None:
        edad_acusado = "buscar_manualmente"
    else:
        edad_acusado = est_civil_acusado.groups(1)[0]
    return edad_acusado

def to_excel(
    violencia_de_genero,
    violencia_fisica,
    genero_denunciante,
    nacionalidad_denunciante,
    est_civil_denunciante,
    edad_denunciante,
    est_denunciante,
    domic_denunciante,
    villa_denunciante,
    ocupac_denunciante,
    genero_acusado,
    nacionalidad_acusado,
    est_civil_acusado,
    edad_acusado,
):
    pd.set_option("display.max_columns", None)
    resumen = pd.DataFrame(
        {
            "violencia_de_genero": violencia_de_genero,
            "ocupac_denunciante": ocupac_denunciante,
            "villa_denunciante": villa_denunciante,
            "domic_denunciante": domic_denunciante,
            "est_denunciante": est_denunciante,
            "edad_denunciante": edad_denunciante,
            "est_civil_denunciante": est_civil_denunciante,
            "nacionalidad_denunciante": nacionalidad_denunciante,
            "violencia_fisica": violencia_fisica,
            "genero_denunciante": genero_denunciante,
            "genero_acusado": genero_acusado,
            "nacionalidad_acusado": nacionalidad_acusado,
            "est_civil_acusado": est_civil_acusado,
            "edad_acusado": edad_acusado,
            "zona_hecho": "buscar_manualmente",
            "lugar_hecho": "buscar_manualmente",
            "villa_hecho": "buscar_manualmente",
            "tipo_armas": "buscar_manualmente",
            "arma_secuestrada": "buscar_manualmente",
            "fecha_inicio": "buscar_manualmente",
            "fecha_fin": "buscar_manualmente",
            "modalidad_violencia": "doméstica",  # por ser de la OVD le colocamos doméstica a todas
        },
        index=[0],
    )
    resumen.replace(" ", "_", regex=True, inplace=True)
    return resumen
