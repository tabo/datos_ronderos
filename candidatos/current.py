import concurrent.futures
from queue import Queue
from functools import partial
from dataclasses import dataclass, field
from typing import Any

from candidatos.requests import get_client
from candidatos import cache


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    fun: Any = field(compare=False)


class Current:
    def __init__(self, queue: Queue):
        self.queue = queue
        self.env = {}

    def qput(self, priority, meth, **kwargs):
        key = self.cache_key(meth, **kwargs)
        if not cache.is_memory_cached(key):
            self.queue.put(
                PrioritizedItem(priority=priority, fun=partial(meth, **kwargs))
            )

    def load(self):
        self.env = (self.config("environment-config"))["env"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            mapper = {
                executor.submit(self.config, config_type): config_type
                for config_type in self._config_urls().keys()
            }
            configs = {
                mapper[future]: future.result()
                for future in concurrent.futures.as_completed(mapper)
            }
        tipos_elecciones = {
            x["idTipoEleccion"]
            for x in configs["config"]["tipoEleccion"].values()
            if x is not None
        }
        elecciones_ids = [
            (val["idProcesoElectoral"], tipo_eleccion)
            for key, val in configs["config"].items()
            for tipo_eleccion in tipos_elecciones
            if key.startswith("proceso")
        ]
        for proceso_electoral, tipo_eleccion in elecciones_ids:
            self.qput(
                10,
                self.listas_regio_muni,
                id_proceso_electoral=proceso_electoral,
                id_tipo_eleccion=tipo_eleccion,
            )
        self.queue.join()

    @cache.cachejson("current", "config", "")
    def config(self, config_type: str):
        if config_type == "environment-config":
            url = (
                "https://plataformaelectoral.jne.gob.pe/assets/environment-config.json"
            )
        else:
            url = self._config_urls()[config_type]
        with get_client() as client:
            resp = client.get(url)
            return resp.json()

    # getBuscarCandidato(e, a) {
    #     const i = {
    #         pageSize: e.pageSize,
    #         skip: e.skip,
    #         filter: Object.assign({}, a)
    #     };
    #     return this.http.post(`${this.apiUrl}/api/v1/candidato`, i)
    # }

    # getHojaVidaCandidato(e) {
    #     return this.http.get(`${this.apiUrl7}/api/v1/candidato/hoja-vida?IdHojaVida=${e}`)
    # }

    def candidato_hojavida(self, id_hoja_vida: int):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath7"] + "/api/v1/candidato/hoja-vida",
                    params={"IdHojaVida": id_hoja_vida},
                )
                return resp.json()

        res = cache.get_value(
            self.cache_key(self.candidato_hojavida, id_hoja_vida=id_hoja_vida), _fn
        )
        if res is not None:
            dg = res["datoGeneral"]
            self.qput(
                20,
                self.candidato_plan,
                id_proceso_electoral=int(dg["idProcesoElectoral"]),
                id_tipo_eleccion=int(dg["idTipoEleccion"]),
                id_organizacion_politica=int(dg["idOrganizacionPolitica"]),
                id_solicitud_lista=int(dg["idSolicitudLista"]),
            )
        return res

    candidato_hojavida.cache_base = ("current", "candidatos-hojavidas", "hojavida")

    # ObtenerAnotacionMarginal(e) {
    #     return this.http.get(`${this.apiUrl2}/api/v1/candidato/anotacion-marginal?IdHojaVida=${e}`)
    # }
    def candidato_anotacion_marginal(self, id_hoja_vida: int):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath2"] + "/api/v1/candidato/anotacion-marginal",
                    params={"IdHojaVida": id_hoja_vida},
                )
                return resp.json()

        return cache.get_value(
            self.cache_key(
                self.candidato_anotacion_marginal, id_hoja_vida=id_hoja_vida
            ),
            _fn,
        )

    candidato_anotacion_marginal.cache_base = (
        "current",
        "candidatos-anotaciones-marginales",
        "anotacion-marginal",
    )

    # ObtenerExpedientesRelacionado(e) {
    #     return this.http.get(`${this.apiUrl5}/api/v1/candidato/expediente?IdHojaVida=${e}`)
    # }
    def candidato_expedientes_relacionados(self, id_hoja_vida: int):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath5"] + "/api/v1/candidato/expediente",
                    params={"IdHojaVida": id_hoja_vida},
                )
                return resp.json()

        return cache.get_value(
            self.cache_key(
                self.candidato_expedientes_relacionados, id_hoja_vida=id_hoja_vida
            ),
            _fn,
        )

    candidato_expedientes_relacionados.cache_base = (
        "current",
        "candidatos-expedientes",
        "expedientes",
    )

    # BuscarPlanGobiernos(e, a) {
    #     const i = {
    #         pageSize: e.pageSize,
    #         skip: e.skip,
    #         filter: Object.assign({}, a)
    #     };
    #     return this.http.post(`${this.apiUrl8}/api/v1/plan-gobierno/busqueda-avanzada`, i)
    # }

    # BuscarDadivas(e) {
    #     return this.http.post(`${this.apiUrl2}/api/v1/candidato/expediente-dadivas`, e)
    # }

    # BuscarListas(e) {
    #     return this.http.post(`${this.apiUrl}/api/v1/candidato/listas`, e)
    # }

    # BuscarListasRegionalesMunicipales(e) {
    #     return this.http.post(`${this.apiUrl5}/api/v1/candidato/listas-regio-muni`, e)
    # }
    def listas_regio_muni(self, id_proceso_electoral: int, id_tipo_eleccion: int):
        def _fn():
            with get_client() as client:
                resp = client.post(
                    self.env["apiPath5"] + "/api/v1/candidato/listas-regio-muni",
                    json={
                        "pageSize": 0,
                        "skip": 0,
                        "sortField": "",
                        "sortDir": "",
                        "filter": {
                            "idProcesoElectoral": id_proceso_electoral,
                            "idTipoEleccion": id_tipo_eleccion,
                            "idOrganizacionPolitica": 0,
                            "idJuradoElectoral": 0,
                            "txUbigeoDepartamento": "00",
                            "txUbigeoProvincia": "00",
                            "txUbigeoDistrito": "00",
                        },
                    },
                )
                return resp.json()

        key = self.cache_key(
            self.listas_regio_muni,
            id_proceso_electoral=id_proceso_electoral,
            id_tipo_eleccion=id_tipo_eleccion,
        )
        res = cache.get_value(key, _fn)
        if res is not None:
            for lista in res["data"]:
                plan_gobierno_id = lista["idPlanGobierno"]
                expediente = lista["txCodExpedienteExt"]
                self.qput(30, self.detalle_plan, id_plan_gobierno=plan_gobierno_id)
                self.qput(40, self.expediente_detalle, expediente=expediente)
                self.qput(50, self.expediente_directo, expediente=expediente)
                self.qput(
                    60,
                    self.expediente_candidatos,
                    id_proceso_electoral=id_proceso_electoral,
                    expediente=expediente,
                )
                self.qput(90, self.expediente_hijo, expediente=expediente)
        return res

    listas_regio_muni.cache_base = (
        "current",
        "listas-regio-muni",
        "listas-regio-muni",
    )

    # VerDetallePlan(e) {
    #     return this.http.get(`${this.apiUrl2}/api/v1/plan-gobierno/detalle?IdPlanGobierno=${e}`)
    # }
    def detalle_plan(self, id_plan_gobierno: int):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath2"] + "/api/v1/plan-gobierno/detalle",
                    params={
                        "IdPlanGobierno": id_plan_gobierno,
                    },
                )
                return resp.json()

        return cache.get_value(
            self.cache_key(self.detalle_plan, id_plan_gobierno=id_plan_gobierno), _fn
        )

    detalle_plan.cache_base = ("current", "candidatos-planes", "plan")

    # VerDetallePlanxCanditato(e) {
    #     return this.http.get(`${this.apiUrl8}/api/v1/plan-gobierno/detalle-para-candidato?IdProcesoElectoral=${e.IdProcesoElectoral}&IdTipoEleccion=${e.IdTipoEleccion}&IdOrganizacionPolitica=${e.IdOrganizacionPolitica}&IdSolicitudLista=${e.IdSolicitudLista}`)
    # }
    def candidato_plan(
        self,
        id_proceso_electoral: int,
        id_tipo_eleccion: int,
        id_organizacion_politica: int,
        id_solicitud_lista: int,
    ):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath8"]
                    + "/api/v1/plan-gobierno/detalle-para-candidato",
                    params={
                        "IdProcesoElectoral": id_proceso_electoral,
                        "IdTipoEleccion": id_tipo_eleccion,
                        "IdOrganizacionPolitica": id_organizacion_politica,
                        "IdSolicitudLista": id_solicitud_lista,
                    },
                )
                return resp.json()

        return cache.get_value(
            self.cache_key(
                self.candidato_plan,
                id_proceso_electoral=id_proceso_electoral,
                id_tipo_eleccion=id_tipo_eleccion,
                id_organizacion_politica=id_organizacion_politica,
                id_solicitud_lista=id_solicitud_lista,
            ),
            _fn,
        )

    candidato_plan.cache_base = ("current", "candidatos-planes-para-candidato", "plan")

    # VerCandidatosXExpedientes(e, a) {
    #     return this.http.get(`${this.apiUrl2}/api/v1/plan-gobierno/candidatos?IdProcesoElectoral=${a}&TxCodExpedienteExt=${e}`)
    # }
    def expediente_candidatos(self, id_proceso_electoral: int, expediente: str):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath2"] + "/api/v1/plan-gobierno/candidatos",
                    params={
                        "IdProcesoElectoral": id_proceso_electoral,
                        "TxCodExpedienteExt": expediente,
                    },
                )
                return resp.json()

        return cache.get_value(
            self.cache_key(
                self.expediente_candidatos,
                id_proceso_electoral=id_proceso_electoral,
                expediente=expediente,
            ),
            _fn,
        )

    expediente_candidatos.cache_base = (
        "current",
        "expedientes-candidatos",
        "candidatos",
    )

    # descargarArchivoByGuid(e) {
    #     return this.http.get(`${this.apiUrlGestionDocumento}/api/v1/gestion-documentos/archivos/download?guidArchivo=${e}&token=${this.token}&moduloHash=${this.moduloHash}`, {
    #         responseType: "blob"
    #     })
    # }

    # BuscarResolucionPorCandidato(e) {
    #     return this.http.get(`${this.apiUrl6}/api/v1/resolucion/busqueda-candidato/${e}`)
    # }

    # getProvinciaPorDepartamentoJuradoElectoral(e, a) {
    #     return this.http.get(`${this.apiUrl2}/api/v1/ubigeo/provincia/jurado-electoral/${e}/${a}`)
    # }

    # getProvinciaPorDepartamento(e) {
    #     return this.http.get(`${this.apiUrl}/api/v1/ubigeo/provincia/${e}`)
    # }

    # getDepartamentoPorJuradoElectoral(e) {
    #     return this.http.get(`${this.apiUrl3}/api/v1/ubigeo/departamento/jurado-electoral/${e}`)
    # }

    # getDistritoPorProvincia(e, a) {
    #     return this.http.get(`${this.apiUrl}/api/v1/ubigeo/distrito/provincia/${e}/${a}`)
    # }

    # getBuscarCandidatos(e, a) {
    #     const i = {
    #         pageSize: e.pageSize,
    #         skip: e.skip,
    #         filter: Object.assign({}, a)
    #     };
    #     return this.http.post(`${this.apiUrl4}/api/v1/candidato/avanzada`, i)
    # }

    # getBuscarExpediente(e) {
    #     return this.http.get(`${this.apiUrl}/api/v1/expediente/detalle?CodExpedienteExt=${e}`)
    # }
    def expediente_detalle(self, expediente: str):
        def _fn():
            with get_client() as client:
                resp = client.get(
                    self.env["apiPath"] + "/api/v1/expediente/detalle",
                    params={"CodExpedienteExt": expediente},
                )
                data = resp.json()
                assert "datoGeneral" in data
                return data

        res = cache.get_value(
            self.cache_key(self.expediente_detalle, expediente=expediente), _fn
        )
        if res is not None:
            for x in res["expedienteCandidato"]:
                id_proceso_electoral = int(x["idProcesoElectoral"])
                id_candidato = int(x["idCandidato"])
                id_hoja_vida = int(x["idHojaVida"])
                self.qput(12, self.candidato_hojavida, id_hoja_vida=id_hoja_vida)
                self.qput(
                    14, self.candidato_anotacion_marginal, id_hoja_vida=id_hoja_vida
                )
                self.qput(
                    16,
                    self.candidato_expedientes_relacionados,
                    id_hoja_vida=id_hoja_vida,
                )
                self.qput(
                    18,
                    self.candidato_requisito,
                    id_proceso_electoral=id_proceso_electoral,
                    id_candidato=id_candidato,
                )
        return res

    expediente_detalle.cache_base = ("current", "expedientes-detalles", "detalle")

    # getBuscarExpedienteHijo(e, t) {
    #     const i = {
    #         pageSize: e.pageSize,
    #         skip: e.skip,
    #         filter: Object.assign({}, t)
    #     };
    #     return this.http.post(`${this.apiUrl3}/api/v1/expediente/consulta-expediente-hijo`, i)
    # }
    def expediente_hijo(self, expediente: str):
        def _fn():
            with get_client() as client:
                resp = client.post(
                    self.env["apiPath3"]
                    + "/api/v1/expediente/consulta-expediente-hijo",
                    json={
                        "pageSize": 9999999,
                        "skip": 1,
                        "filter": {"strCodigo": expediente},
                    },
                )
                data = resp.json()
                assert (int(data["count"]) == 0 and data["totalPages"] == 0) or data[
                    "totalPages"
                ] == 1
                return data

        return cache.get_value(
            self.cache_key(self.expediente_hijo, expediente=expediente), _fn
        )

    expediente_hijo.cache_base = ("current", "expedientes-hijos", "hijo")

    # getBuscarExpedienteDirecto(e, t) {
    #     const i = {
    #         pageSize: e.pageSize,
    #         skip: e.skip,
    #         filter: Object.assign({}, t)
    #     };
    #     return this.http.post(`${this.apiUrl3}/api/v1/expediente/consulta-expediente-directo`, i)
    # }
    def expediente_directo(self, expediente: str):
        def _fn():
            with get_client() as client:
                resp = client.post(
                    self.env["apiPath3"]
                    + "/api/v1/expediente/consulta-expediente-directo",
                    json={
                        "pageSize": 9999999,
                        "skip": 1,
                        "filter": {"strCodigo": expediente},
                    },
                )
                data = resp.json()
                assert (int(data["count"]) == 0 and data["totalPages"] == 0) or data[
                    "totalPages"
                ] == 1
                return data

        return cache.get_value(
            self.cache_key(self.expediente_directo, expediente=expediente), _fn
        )

    expediente_directo.cache_base = ("current", "expedientes-directos", "directo")

    # BuscarResolucionPorExpediente(e) {
    #     return this.http.get(`${this.apiUrl5}/api/v1/resolucion/busqueda-expediente/${e}`)
    # }

    # descargarArchivo(e) {
    #     return null
    # }

    # getRequsitoByIdCandidato(e) {
    #     return this.http.post(`${this.apiUrl2}/api/v1/expediente/candidato-requisito`, e)
    # }
    def candidato_requisito(self, id_proceso_electoral: int, id_candidato: int):
        def _fn():
            with get_client() as client:
                resp = client.post(
                    self.env["apiPath2"] + "/api/v1/expediente/candidato-requisito",
                    json={
                        "idProcesoElectoral": id_proceso_electoral,
                        "idCandidato": id_candidato,
                    },
                )
                return resp.json()

        return cache.get_value(
            self.cache_key(
                self.candidato_requisito,
                id_proceso_electoral=id_proceso_electoral,
                id_candidato=id_candidato,
            ),
            _fn,
        )

    candidato_requisito.cache_base = ("current", "candidatos-requisitos", "requisito")

    @classmethod
    def cache_key(cls, fn, **kwargs):
        dataset, category, key_base = fn.cache_base
        return cache.get_key(dataset, category, key_base, **kwargs)

    def _config_urls(self):
        return {
            # getSettings() {
            #     return this.http.get(`${this.apiUrl}/api/v1/config`)
            # }
            "config": self.env["apiPath"] + "/api/v1/config",
            # getOrganizacionPolitica() {
            #     return this.http.get(`${this.apiUrl2}/api/v1/organizacion-politica`)
            # }
            "organizacion-politica": self.env["apiPath2"]
            + "/api/v1/organizacion-politica",
            # getJuradoElectoral() {
            #     return this.http.get(`${this.apiUrl2}/api/v1/jurado-electoral`)
            # }
            "jurado-electoral": self.env["apiPath2"] + "/api/v1/jurado-electoral",
            # getTipoEleccion() {
            #     return this.http.get(`${this.apiUrl2}/api/v1/tipo-eleccion`)
            # }
            "tipo-eleccion": self.env["apiPath2"] + "/api/v1/tipo-eleccion",
            # getExperienciaLaboral() {
            #     return this.http.get(`${this.apiUrl3}/api/v1/experiencia-laboral`)
            # }
            "experiencia-laboral": self.env["apiPath3"] + "/api/v1/experiencia-laboral",
            # getCargoEleccion() {
            #     return this.http.get(`${this.apiUrl2}/api/v1/cargo-eleccion`)
            # }
            "cargo-eleccion": self.env["apiPath2"] + "/api/v1/cargo-eleccion",
            # getExpedienteDadiva() {
            #     return this.http.get(`${this.apiUrl3}/api/v1/expediente-dadiva`)
            # }
            "expediente-dadiva": self.env["apiPath3"] + "/api/v1/expediente-dadiva",
            # getGradoAcademico() {
            #     return this.http.get(`${this.apiUrl3}/api/v1/grado-academico`)
            # }
            "grado-academico": self.env["apiPath3"] + "/api/v1/grado-academico",
            # getSentenciaDeclarada() {
            #     return this.http.get(`${this.apiUrl2}/api/v1/sentencia-declarada`)
            # }
            "sentencia-declarada": self.env["apiPath2"] + "/api/v1/sentencia-declarada",
            # getUbigeo() {
            #     return this.http.get(`${this.apiUrl3}/api/v1/ubigeo/consulta`)
            # }
            "ubigeo-consulta": self.env["apiPath3"] + "/api/v1/ubigeo/consulta",
        }
