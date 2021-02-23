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

dias = {
    "primero": "1",
    "dos": "2",
    "tres": "3",
    "cuatro": "4",
    "cinco": "5",
    "seis": "6",
    "siete": "7",
    "ocho": "8",
    "nueve": "9",
    "diez": "10",
    "once": "11",
    "doce": "12",
    "trece": "13",
    "catorce": "14",
    "quince": "15",
    "dieciseis": "16",
    "diecisiete": "17",
    "dieciocho": "18",
    "diecinueve": "19",
    "veinte": "20",
    "veintiuno": "21",
    "veintidos": "22",
    "veintitres": "23",
    "veinticuatro": "24",
    "veinticinco": "25",
    "veintiseis": "26",
    "veintisiete": "27",
    "veintiocho": "28",
    "veintinueve": "29",
    "treinta": "30",
    "treinta y uno": "31",
}


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


def buscar_numero_legajo(data):
    nrolegajo = re.search(r"informe interdisciplinario de.*?legajo.*?(\d+/\d+)", data)
    if nrolegajo == None:
        nrolegajo = "buscar_manualmente"
    else:
        nrolegajo = nrolegajo.groups(1)[0]
    return nrolegajo


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
    est_civil_acusado = re.search(r" de estado civil (.*?),", info_acusado)
    if est_civil_acusado == None:
        est_civil_acusado = "buscar_manualmente"
    else:
        est_civil_acusado = est_civil_acusado.groups(1)[0]
    return est_civil_acusado


def buscar_edad_acusado(info_acusado):
    edad_acusado = re.search(r"de (\d+)\s+\w+\s+de edad", info_acusado)
    if edad_acusado == None:
        edad_acusado = "buscar_manualmente"
    else:
        edad_acusado = edad_acusado.groups(1)[0]
    return edad_acusado


def buscar_instruccion_acusado(info_acusado):
    instruccion_acusado = re.search(r"(estudios .*?etos)", info_acusado)
    if instruccion_acusado == None:
        instruccion_acusado = "buscar_manualmente"
    else:
        instruccion_acusado = instruccion_acusado.groups(1)[0]
    return instruccion_acusado


def buscar_domicilio_acusado(info_acusado):
    domic_acusado = re.search(r"barrio de (\w+\s\w+).*?ciudad.*?,", info_acusado)
    if domic_acusado == None:
        domic_acusado = "buscar_manualmente"
    else:
        domic_acusado = domic_acusado.groups(1)[0].replace(" en", "").replace(" de", "")
        domic_acusado = [
            domic_acusado.replace(key, value)
            for key, value in barr_zona.items()
            if domic_acusado == key
        ]
    return domic_acusado


def buscar_ocupacion_acusado(info_acusado):
    ocupacion_acusado = re.search(
        r"((?:se desempeña|condición laboral|de ocupación|emplead[oa]).*?)[,.]",
        info_acusado,
    )
    if ocupacion_acusado == None:
        ocupacion_acusado = "buscar_manualmente"
    else:
        ocupacion_acusado = ocupacion_acusado.groups(1)[0]
    return re.sub("[%s]" % re.escape(string.punctuation), "", ocupacion_acusado)


def buscar_relacion(info_acusado):
    relacion = re.search(r"relación a su (.*?) el", info_acusado)
    if relacion == None:
        relacion = "buscar_manualmente"
    else:
        relacion = relacion.groups(1)[0]
    return relacion.replace(" no", "").replace(" conviviente", "")


def chequear_conv(info_acusado):
    relacion = re.search(r"relación a su (.*?) el", info_acusado)
    if relacion == None:
        relacion = "buscar_manualmente"
    else:
        relacion = relacion.groups(1)[0]
    if re.search(r"no conviviente", relacion) != None:
        convivencia = "no"
    elif re.search(r"conviviente", relacion) != None:
        convivencia = "si"
    else:
        convivencia = "buscar_manualmente"
    return convivencia


def buscar_vinculos(data):
    info_vinculos = re.search(r"(manifiesta que .*)con relación al episodio", data)
    if info_vinculos == None:
        info_vinculos = "buscar_manualmente"
    else:
        info_vinculos = info_vinculos.groups(1)[0]
    return info_vinculos


def buscar_info_episodio(data):
    info_episodio = re.search(r"(con relación al episodio .*?)seguidamente", data)
    if info_episodio == None:
        info_episodio = "buscar_manualmente"
    else:
        info_episodio = info_episodio.groups(1)[0]
    return info_episodio


def buscar_denuncia_anterior(info_episodio):
    return (
        "buscar_manualmente"
        if re.search(
            r"preguntada acerca de si .*? denuncia anterior.*?:.*?s[ií]", info_episodio
        )
        == None
        else "si"
    )


def buscar_medidas_prot(info_episodio):
    medidas_prot = re.search(r"medidas de protección.*?\:(.*?)\.", info_episodio)
    if medidas_prot != None:
        medidas_prot = medidas_prot[1]
    else:
        medidas_prot = "buscar_manualmente"
    return medidas_prot


def buscar_dia_hecho(data):

    infoepisodio = re.search(r"(con relación al episodio .*?)seguidamente", data)

    if infoepisodio == None:
        infoepisodio = "buscar_manualmente"
    else:
        infoepisodio = infoepisodio.groups(1)[0]

    fecha_hecho = re.search(r"refiere\S?\s+((?:\S+\s*){25})", infoepisodio)
    if fecha_hecho == None:
        fecha_hecho = "buscar_manualmente"
    else:
        fecha_hecho = fecha_hecho.groups(1)[0]

    for key in dias.keys():
        fecha_hecho = fecha_hecho.replace(key, dias[key])

    dia_hecho = re.search(
        r"(lunes|martes|miercoles|jueves|viernes|sabado|domingo)", fecha_hecho
    )

    if dia_hecho == None:
        dia_hecho = "buscar_manualmente"
    else:
        dia_hecho = dia_hecho.groups(1)[0]

    if dia_hecho == "buscar_manualmente":
        DiasSemana = (
            "lunes",
            "martes",
            "miercoles",
            "jueves",
            "viernes",
            "sabado",
            "domingo",
        )
        try:
            dia_fecha = dt.datetime.strptime(dia_numero, "%Y/%m/%d").weekday()
            dia_hecho = DiasSemana[dia_fecha]
        except:
            dia_hecho = "buscar_manualmente"

    return dia_hecho


def buscar_conclusiones(data):
    conclusiones = re.search(r"(valoración de la .*).", data)

    if conclusiones == None:
        conclusiones = "buscar_manualmente"
    else:
        conclusiones = conclusiones.groups(1)[0]

    return conclusiones


def buscar_riesgo(conclusiones):
    riesgo = re.search(r"se valora la misma como de (\w+\s\w+)", conclusiones)
    if riesgo == None:
        riesgo = "buscar_manualmente"
    else:
        riesgo = riesgo.groups(1)[0].replace("riesgo", "").replace(" ", "")

    return riesgo


def buscar_informe_final(data):
    informe_final = re.search(r"(informe interdisciplinario de.*?)$", data)

    if informe_final == None:
        informe_final = "buscar_manualmente"
    else:
        informe_final = informe_final.groups(1)[0]

    return informe_final


def buscar_violencia_psico(informe_final):
    informe_final = informe_final.split(".")
    for linea in informe_final:
        match = re.search(r"violencia(.*?)" + re.escape("psicológica"), linea)
        if match != None:
            match = match[0]
            break
    if match == None:
        match = "buscar_manualmente"

    return match


def buscar_violencia_econ(informe_final):
    informe_final = informe_final.split(".")
    for linea in informe_final:
        match = re.search(r"violencia(.*?)" + re.escape("económica"), linea)
        if match != None:
            match = match[0]
            break
    if match == None:
        match = "buscar_manualmente"

    return match


def buscar_violencia_sex(informe_final):
    informe_final = informe_final.split(".")
    for linea in informe_final:
        match = re.search(r"violencia(.*?)" + re.escape("sexual"), linea)
        if match != None:
            match = match[0]
            break
    if match == None:
        match = "buscar_manualmente"

    return match


def buscar_violencia_soc(informe_final):
    informe_final = informe_final.split(".")
    for linea in informe_final:
        match = re.search(r"violencia(.*?)" + re.escape("social"), linea)
        if match != None:
            match = match[0]
            break
    if match == None:
        match = "buscar_manualmente"

    return match


def buscar_violencia_amb(informe_final):
    informe_final = informe_final.split(".")
    for linea in informe_final:
        match = re.search(r"violencia(.*?)" + re.escape("ambiental"), linea)
        if match != None:
            match = match[0]
            break
    if match == None:
        match = "buscar_manualmente"

    return match


def buscar_violencia_simb(informe_final):
    informe_final = informe_final.split(".")
    for linea in informe_final:
        match = re.search(r"violencia(.*?)" + re.escape("simbólica"), linea)
        if match != None:
            match = match[0]
            break
    if match == None:
        match = "buscar_manualmente"

    return match


def buscar_hijos(informe_final):
    hijos_union = []
    informe_final = informe_final.split(".")

    for linea in informe_final:
        if linea.startswith(" de dicha unión"):
            hijos = re.findall(r"\s(\d+)\saños.*?", linea)
            if hijos:
                hijos_union = hijos

    return hijos_union


def buscar_hijos_en_comun(hijos):
    if len(hijos) == 0:
        hijos_comun = "no"
    elif len(hijos) > 0:
        hijos_comun = "si"
    else:
        hijos_comun = "buscar_manualmente"
    return hijos_comun


def buscar_frecuencia(informe_final):
    frecuencia = re.search(r"(frecuencia\S?\s+(?:\S+\s*){8})", informe_final)

    if frecuencia == None:
        frecuencia = "buscar_manualmente"
    else:
        frecuencia = frecuencia.groups(1)[0]

    return frecuencia


def buscar_dijo(data):
    match = re.findall(
        r"(?:dijo|dice|decía|respondió|responde)\s+[\"“'‘]([^\"“”'‘’]{3,})[\"'”’]",
        data,
    )
    match = " // ".join(map(str, match))
    return match if match else "buscar_manualmente"


def buscar_dijo_sin_comillas(data):
    match = re.findall(
        r"(?:dijo|dice|decía|respondió|responde)\S?\s+((?:\S+\s*){12})", data
    )
    match = " // ".join(map(str, match))

    return match if match else "buscar_manualmente"


def buscar_comillas(data):
    match = re.findall(r"[\"“']([^'\"“”]{3,})[\"”']", data)
    match = " // ".join(map(str, match))
    return match if match else "buscar_manualmente"


def buscar_fecha_del_hecho(info_episodio, fecha_denuncia):

    fecha_hecho = re.search(r"refiere\S?\s+((?:\S+\s*){25})", info_episodio)

    if fecha_hecho == None:
        fecha_hecho = "buscar_manualmente"
    else:
        fecha_hecho = fecha_hecho.groups(1)[0]

    for key in dias.keys():
        fecha_hecho = fecha_hecho.replace(key, dias[key])

    dia_ref_hoy = re.search(
        r"(hoy|ayer|anteayer|anoche|anteanoche|esta tarde|esta mañana|recién)",
        fecha_hecho,
    )

    if dia_ref_hoy == None:
        dia_ref_hoy = "buscar_manualmente"
    else:
        dia_ref_hoy = dia_ref_hoy.groups(1)[0]

    if dia_ref_hoy == "buscar_manualmente":
        try:
            dia_numero = re.search(r"(\d+)\s+de\s+(\w+)", fecha_hecho).groups(1)
            dia_numero = " ".join(map(str, dia_numero))
        except:
            dia_numero = fecha_hecho
    else:
        lista_hoy = ["hoy", "esta tarde", "esta mañana", "recién"]
        lista_ayer = ["ayer", "anoche"]
        lista_anteayer = ["anteayer", "anteanoche"]
        if dia_ref_hoy in lista_hoy:
            dia_numero = fecha_denuncia
        elif dia_ref_hoy in lista_ayer:
            dia_numero = (
                dt.datetime.strptime(fecha_denuncia, "%Y/%m/%d") - dt.timedelta(days=1)
            ).strftime("%Y/%m/%d")
        elif dia_ref_hoy in lista_anteayer:
            dia_numero = (
                dt.datetime.strptime(fecha_denuncia, "%Y/%m/%d") - dt.timedelta(days=2)
            ).strftime("%Y/%m/%d")

    return dia_numero


def buscar_fecha_denuncia(data):
    m = {
        "enero": "01",
        "febrero": "02",
        "marzo": "03",
        "abril": "04",
        "mayo": "05",
        "junio": "06",
        "julio": "07",
        "agosto": "08",
        "septiembre": "09",
        "setiembre": "09",
        "octubre": "10",
        "noviembre": "11",
        "diciembre": "12",
    }

    try:
        fecha_denuncia = re.search(
            r"la ciudad de buenos aires, (?:a|al).*?(\d+) día.*?(?:de|del mes de) (\w+) de.*?(\d+), comparece",
            data,
        ).groups(1)
    except:
        fecha_denuncia = "buscar_manualmente"

    if fecha_denuncia != "buscar_manualmente":
        añodenuncia = fecha_denuncia[2]
        mesdenuncia = str(m[fecha_denuncia[1]])
        diadenuncia = fecha_denuncia[0]
        fechadenuncia = dt.datetime.strptime(
            (añodenuncia + "/" + mesdenuncia + "/" + diadenuncia), "%Y/%m/%d"
        ).strftime("%Y/%m/%d")
    else:
        fechadenuncia = fecha_denuncia

    return fechadenuncia


def buscar_horario_hecho(info_episodio):
    fecha_hecho = re.search(r"refiere\S?\s+((?:\S+\s*){25})", info_episodio)

    if fecha_hecho == None:
        fecha_hecho = "buscar_manualmente"
    else:
        fecha_hecho = fecha_hecho.groups(1)[0]

    for key in dias.keys():
        fecha_hecho = fecha_hecho.replace(key, dias[key])

    horario_hecho = re.search(r"(mañana|tarde|noche|anoche)", fecha_hecho)

    if horario_hecho == None:
        horario_hecho = "buscar_manualmente"
    else:
        horario_hecho = horario_hecho.groups(1)[0]

    horario_hecho = horario_hecho.replace("anoche", "noche")

    return horario_hecho


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
    nacionalida_acusado,
    est_civil_acusado,
    edad_acusado,
    instruccion_acusado,
    domicilio_acusado,
    ocupacion_acusado,
    relacion,
    convivencia,
    denuncia_anterior,
    medidas_prot,
    dia_hecho,
    riesgo,
    violencia_psico,
    violencia_econ,
    violencia_sex,
    violencia_soc,
    violencia_amb,
    violencia_simb,
    hijos_en_comun,
    frecuencia,
    frases_agresion,
    frases_sin_comillas,
    frases_comillas,
    fecha_hecho,
    fecha_denuncia,
    horario_hecho,
    numero_legajo,
):
    pd.set_option("display.max_columns", None)
    resumen = pd.DataFrame(
        {
            "VIOLENCIA_DE_GENERO": violencia_de_genero,
            "V_FISICA": violencia_fisica,
            "V_PSIC": violencia_psico,
            "V_ECON": violencia_econ,
            "V_SEX": violencia_sex,
            "V_SOC": violencia_soc,
            "V_AMB": violencia_amb,
            "V_SIMB": violencia_simb,
            "FRASES_AGRESION": frases_agresion,
            "FRASES_DIJO_COMILLAS": frases_comillas,
            "FRASES_DIJO_SIN_COMILLAS": frases_sin_comillas,
            "GENERO_ACUSADO/A": genero_acusado,
            "NACIONALIDAD_ACUSADO/A": nacionalida_acusado,
            "ESTADO_CIVIL_ACUSADO/A": est_civil_acusado,
            "EDAD_ACUSADO/A ": edad_acusado,
            "NIVEL_INSTRUCCION_ACUSADO/A": instruccion_acusado,
            "DOMICILIO_ACUSADO/A": domicilio_acusado,
            "OCUPACION_ACUSADO/A": ocupacion_acusado,
            "GENERO_DENUNCIANTE": genero_denunciante,
            "NACIONALIDAD_DENUNCIANTE": nacionalidad_denunciante,
            "ESTADO_CIVIL_DENUNCIANTE": est_civil_denunciante,
            "EDAD_DENUNCIANTE": edad_denunciante,
            "NIVEL_INSTRUCCION_DENUNCIANTE": est_denunciante,
            "DOMICILIO_DENUNCIANTE": domic_denunciante,
            "ASENTAMIENTO_O_VILLA": villa_denunciante,
            "OCUPACION_DENUNCIANTE": ocupac_denunciante,
            "FRECUENCIA_EPISODIOS": frecuencia,
            "RELACION_Y_TIPO_ENTRE_ACUSADO/A_Y_DENUNCIANTE": relacion,
            "HIJOS_EN_COMUN": hijos_en_comun,
            "EDADES_HIJOS_EN_COMUN": "",
            "CONVIVIENTES": convivencia,
            "DENUNCIA_O_INTV_JUDICIAL_PREVIA": denuncia_anterior,
            "MEDIDAS_DE_PROTECCION_VIGENTES_AL_MOMENTO_DEL_HECHO": medidas_prot,
            "ZONA_DEL_HECHO": "buscar_manualmente",
            "LUGAR_DEL_HECHO": "buscar_manualmente",
            "USO_DE_ARMAS": "buscar_manualmente",
            "TIPO_DE_ARMAS": "buscar_manualmente",
            "DETALLE_ARMA_SECUESTRADA": "buscar_manualmente",
            "FECHA_DEL_HECHO": fecha_hecho,
            "DIA_DEL_HECHO": dia_hecho,
            "HORARIO_DEL_HECHO": horario_hecho,
            "FECHA_DE_INICIO_DEL_HECHO": "buscar_manualmente",
            "FECHA_DE_FINALIZACION_DEL_HECHO": "buscar_manualmente",
            "MODALIDAD_VIOLENCIA": "doméstica",
            "NRO_LEGAJO_OVD": numero_legajo,
            "FECHA_DENUNCIA": fecha_denuncia,
            "RIESGO_OVD": riesgo,
        },
        index=[0],
    )
    resumen.replace(" ", "_", regex=True, inplace=True)
    return resumen
