import typing
from typing import Optional, Tuple, Mapping, Any

import requests
import geojson
import fastapi
import logging
import urllib.parse

from requests import Response

import isb_web.config
from isb_lib.core import MEDIA_JSON

BASE_URL = isb_web.config.Settings().solr_url
_RPT_FIELD = "producedBy_samplingSite_location_rpt"
LONGITUDE_FIELD = "producedBy_samplingSite_location_longitude"
LATITUDE_FIELD = "producedBy_samplingSite_location_latitude"

DEFAULT_COLLECTION_NAME = "isb_core_records"

# Identify the bounding boxes for solr and leaflet for diagnostic purposes
SOLR_BOUNDS = -1
LEAFLET_BOUNDS = -2

MIN_LAT = "min_lat"
MAX_LAT = "max_lat"
MIN_LON = "min_lon"
MAX_LON = "max_lon"

# 0.2 seems ok for the grid cells.
_GEOJSON_ERR_PCT = 0.2
# 0.1 for the leaflet heatmap tends to generate more cells for
# the heatmap “blob” generation
_LEAFLET_ERR_PCT = 0.1

# Maximum rows to return in a streaming request.
# Note that this limit should vary by the number of fields being returned since
# that somewhat dictates memory use for constructing the stream
MAX_STREAMING_ROWS = 500000

# Max rows for a reliquery response
MAX_RELIQUERY_ROWS = 10000

RESERVED_CHAR_LIST = [
    "+",
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
]

ALLOWED_SELECT_METHODS = [
    "search",
    "random",
]

L = logging.getLogger("ISB_SOLR_QUERY")


def escape_solr_query_term(term):
    """Escape a query term for inclusion in a query."""
    term = term.replace("\\", "\\\\")
    for c in RESERVED_CHAR_LIST:
        term = term.replace(c, r"\{}".format(c))
    return term


def clip_float(v, min_v, max_v):
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def get_solr_url(path_component: str):
    return urllib.parse.urljoin(BASE_URL, path_component)


def set_default_params(params, defs, dict: bool = False):
    for k in defs.keys():
        fnd = False
        for row in params:
            if k == row[0]:
                fnd = True
                break
        if not fnd:
            if not dict:
                params.append([k, defs[k]])
            else:
                params[k] = defs[k]
    return params


def replace_param_value(params: list, replacement: dict) -> list:
    for k in replacement.keys():
        for row in params:
            if k == row[0]:
                row[1] = replacement[k]
    return params


def read_param_value(params: list, key: str) -> Optional[Any]:
    for row in params:
        if key == row[0]:
            return row[1]
    return None


solr_api_defparams = {
    "wt": "json",
    "q": "*:*",
    "fl": "id",
    "rows": 10,
    "start": 0,
}


def get_solr_params_from_request(request: fastapi.Request, defparams: dict = solr_api_defparams, supported_params: Optional[list[str]] = None) -> Tuple[list[list[str]], dict]:
    """Turns a GET request into a list of parameters suitable for querying Solr."""
    if request.method != "GET":
        raise ValueError("get_solr_params_from_request only works with GET requests.")

    # Construct a list of K,V pairs to hand on to the solr request.
    # Can't use a standard dict here because we need to support possible
    # duplicate keys in the request query string.
    properties = {
        "q": defparams["q"]
    }
    params = []
    # Update params with the provided parameters
    for k, v in request.query_params.multi_items():
        if supported_params is None or k in supported_params:
            params.append([k, v])
            if k in properties:
                properties[k] = v
    params = set_default_params(params, defparams)
    return params, properties


def get_solr_params_from_request_as_dict(request: fastapi.Request, defparams: dict = solr_api_defparams, supported_params: Optional[list[str]] = None) -> Tuple[dict, dict]:
    """Turns a GET request into a list of parameters suitable for querying Solr."""
    if request.method != "GET":
        raise ValueError("get_solr_params_from_request only works with GET requests.")

    # Construct a list of K,V pairs to hand on to the solr request.
    # Can't use a standard dict here because we need to support possible
    # duplicate keys in the request query string.
    properties = {
        "q": defparams["q"]
    }
    params = {}
    # Update params with the provided parameters
    for k, v in request.query_params.multi_items():
        if supported_params is None or k in supported_params:
            params[k] = v
            if k in properties:
                properties[k] = v
    params = set_default_params(params, defparams, True)
    return params, properties


def _get_heatmap(
    q: str,
    bb: typing.Dict,
    dist_err_pct: float,
    fq: str = "",
    grid_level=None,
) -> typing.Dict:
    # TODO: dealing with the antimeridian ("dateline") in the Solr request.
    # Should probably do this by computing two requests when the request BB
    # straddles the antimeridian.
    if bb is None or len(bb) < 2:
        bb = {MIN_LAT: -90.0, MAX_LAT: 90.0, MIN_LON: -180.0, MAX_LON: 180.0}
    bb[MIN_LAT] = clip_float(bb[MIN_LAT], -90.0, 90.0)
    bb[MAX_LAT] = clip_float(bb[MAX_LAT], -90.0, 90.0)
    bb[MIN_LON] = clip_float(bb[MIN_LON], -180.0, 180.0)
    bb[MAX_LON] = clip_float(bb[MAX_LON], -180.0, 180.0)
    # logging.warning(bb)
    headers = {"Accept": MEDIA_JSON}
    params: dict = {
        "q": q,
        "rows": 0,
        "wt": "json",
        "facet": "true",
        "facet.heatmap": _RPT_FIELD,
        "facet.heatmap.distErrPct": dist_err_pct,
        # "facet.heatmap.gridLevel": grid_level,
        "facet.heatmap.geom": (
            _solr_heatmap_geom_params_str(bb)
        ),
    }
    if fq is not None:
        params["fq"] = fq
    # if grid level is None, then Solr calculates an "appropriate" grid scale
    # based on the bounding box and distErrPct. Seems a bit off...
    if grid_level is not None:
        params["facet.heatmap.gridLevel"] = grid_level
    # Get the solr heatmap for the provided bounds
    url = get_solr_url("select")
    response = requests.get(url, headers=headers, params=params)

    # logging.debug("Got: %s", response.url)
    res = response.json()
    total_matching = res.get("response", {}).get("numFound", 0)
    hm = res.get("facet_counts", {}).get("facet_heatmaps", {}).get(_RPT_FIELD, {})
    hm["numDocs"] = total_matching
    return hm


def _solr_heatmap_geom_params_str(bb):
    return f"[{bb[MIN_LON]} {bb[MIN_LAT]}" f" TO {bb[MAX_LON]} {bb[MAX_LAT]}]"


##
# Create a GeoJSON rendering of the Solr Heatmap response.
# Generates a GeoJSON polygon (rectangle) feature for each Solr heatmap cell
# that has a count value over 0.
# Returns the generated features as a GeoJSON FeatureCollection,
# https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
def solr_geojson_heatmap(
    q, bb, fq=None, grid_level=None, show_bounds=False, show_solr_bounds=False
):
    hm = _get_heatmap(q, bb, _GEOJSON_ERR_PCT, fq=fq, grid_level=grid_level)
    # print(hm)
    gl = hm.get("gridLevel", -1)
    # logging.warning(hm)
    d_lat = hm["maxY"] - hm["minY"]
    dd_lat = d_lat / (hm["rows"])
    d_lon = hm["maxX"] - hm["minX"]
    dd_lon = d_lon / (hm["columns"])
    lat_0 = hm["maxY"]  # - dd_lat
    lon_0 = hm["minX"]  # + dd_lon
    _max_value = 0

    # Container for the generated geojson features
    grid = []
    if show_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    [
                        (
                            bb[MIN_LAT],
                            bb[MIN_LON],
                        ),
                        (
                            bb[MAX_LAT],
                            bb[MIN_LON],
                        ),
                        (
                            bb[MAX_LAT],
                            bb[MAX_LON],
                        ),
                        (
                            bb[MIN_LAT],
                            bb[MAX_LON],
                        ),
                        (
                            bb[MIN_LAT],
                            bb[MIN_LON],
                        ),
                    ]
                ]
            ),
            properties={"count": LEAFLET_BOUNDS},
        )
        grid.append(bbox)
    if show_solr_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    [
                        (
                            hm["minX"],
                            hm["minY"],
                        ),
                        (
                            hm["maxX"],
                            hm["minY"],
                        ),
                        (
                            hm["maxX"],
                            hm["maxY"],
                        ),
                        (
                            hm["minX"],
                            hm["maxY"],
                        ),
                        (
                            hm["minX"],
                            hm["minY"],
                        ),
                    ]
                ]
            ),
            properties={"count": SOLR_BOUNDS},
        )
        grid.append(bbox)

    # Process the Solr heatmap response. Draw a box for each cell
    # that has a count > 0 and set the "count" property of the
    # feature to that value.
    _total = 0
    _count_matrix = hm.get("counts_ints2D", None)
    if _count_matrix is not None:
        for i_row in range(0, hm["rows"]):
            for i_col in range(0, hm["columns"]):
                if _count_matrix[i_row] is not None:
                    v = _count_matrix[i_row][i_col]
                    if v > 0:
                        _total = _total + v
                        if v > _max_value:
                            _max_value = v
                        p0lat = lat_0 - dd_lat * i_row
                        p0lon = lon_0 + dd_lon * i_col
                        pts = geojson.Polygon(
                            [
                                [
                                    (
                                        p0lon,
                                        p0lat,
                                    ),
                                    (
                                        p0lon + dd_lon,
                                        p0lat,
                                    ),
                                    (
                                        p0lon + dd_lon,
                                        p0lat - dd_lat,
                                    ),
                                    (
                                        p0lon,
                                        p0lat - dd_lat,
                                    ),
                                    (
                                        p0lon,
                                        p0lat,
                                    ),
                                ]
                            ]
                        )
                        feature = geojson.Feature(geometry=pts, properties={"count": v})
                        grid.append(feature)
    geodata = geojson.FeatureCollection(grid)
    geodata["max_count"] = _max_value
    geodata["grid_level"] = gl
    geodata["total"] = _total
    geodata["num_docs"] = hm.get("numDocs", 0)
    return geodata


# Generate a list of [latitude, longitude, value] from
# a solr heatmap. Latitude and longitude represent the
# centers of the solr heatmap grid cells. The value is the count
# for the grid cell.
# Suitable for consumption by leaflet: https://leafletjs.com
def solr_leaflet_heatmap(q, bb, fq=None, grid_level=None):
    hm = _get_heatmap(q, bb, _LEAFLET_ERR_PCT, fq=fq, grid_level=grid_level)
    # logging.warning(hm)
    d_lat = hm["maxY"] - hm["minY"]
    dd_lat = d_lat / (hm["rows"])
    d_lon = hm["maxX"] - hm["minX"]
    dd_lon = d_lon / (hm["columns"])
    lat_0 = hm["maxY"] - dd_lat / 2.0
    lon_0 = hm["minX"] + dd_lon / 2.0
    data = []
    max_value = 0
    _total = 0
    for i_row in range(0, hm["rows"]):
        for i_col in range(0, hm["columns"]):
            if hm["counts_ints2D"][i_row] is not None:
                v = hm["counts_ints2D"][i_row][i_col]
                if v > 0:
                    _total = _total + v
                    lt = lat_0 - dd_lat * i_row
                    lg = lon_0 + dd_lon * i_col
                    data.append([lt, lg, v])
                    if v > max_value:
                        max_value = v
    # return list of [lat, lon, count] and maximum count value
    return {
        "data": data,
        "max_value": max_value,
        "total": _total,
        "num_docs": hm.get("numDocs", 0),
    }


def solr_query(params, query=None, handler: str = "select", wrap_response: bool = True):
    """
    Issue a request against the solr select endpoint.

    Params is a list of [k, v] to support duplicate keys, which solr
    uses a lot of.

    Args:
        params: list of list, see https://solr.apache.org/guide/8_9/common-query-parameters.html

    Returns:
        Iterator for the solr response.
    """
    url = get_solr_url(handler)
    headers = {"Accept": MEDIA_JSON}
    content_type = MEDIA_JSON
    wt_map = {
        "csv": "text/plain",
        "xml": "application/xml",
        "geojson": "application/geo+json",
        "smile": "application/x-jackson-smile",
        "json": "application/json",
    }
    if type(params) is list:
        for k, v in params:
            if k == "wt":
                content_type = wt_map.get(v.lower(), "json")

    if query is None:
        response = requests.get(url, headers=headers, params=params, stream=True)
    else:
        response = requests.post(
            url, headers=headers, params=params, json=query, stream=True
        )
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=2048), media_type=content_type
    )


def reliquery_solr_query(query: str) -> dict:
    """
    Returns the solr response from making the reliquery query
    Args:
        query: The solr query to execute

    Returns: The solr response from executing the reliquery query
    """
    url = get_solr_url("select")
    headers = {"Accept": MEDIA_JSON}
    params: Mapping = {
        "q": query,
        "rows": MAX_RELIQUERY_ROWS,
        "wt": "json",
        "fl": "id"
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()


def solr_get_record(identifier):
    """
    Retrieve the solr document for the specified identifier.

    The select endpoint is used instead of get because get does
    not return fields populated by copyField operations.

    Args:
        identifier: string, the record identifier

    Returns: status_code, object
    """
    params = {
        "wt": "json",
        "q": f"id:{escape_solr_query_term(identifier)}",
        "fl": "*",
        "rows": 1,
        "start": 0,
    }
    url = get_solr_url("select")
    headers = {"Accept": MEDIA_JSON}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        return response.status_code, None
    docs = response.json()
    if docs["response"]["numFound"] == 0:
        return 404, None
    return 200, docs["response"]["docs"][0]


def solr_searchStream(params: list[list[str]], collection: str = DEFAULT_COLLECTION_NAME) -> Response:  # noqa: C901
    """
    Requests a streaming search response from solr.

    params is a list of [key, value]

    The usual q, fq, fl, sort and other standard parameters are accepted.

    Only records that have longitude and latitude are returned.

    The response always includes at least the fields except if
    "xycount" is true (below):
      id: the record id
      x: longitude
      y: latitude

    If "xycount" is provided as a truthy value, then the results include only
    the fields "x,y,n" where n is the number of records at x,y.

    Sorting by distance from a location can be done with the geodist() function, however the
    function result must be included in the streaming response field list.

    For example, given latitude=-17.47833 and longitude=-149.92189, a request
    to stream results in order of distance from that location:

        gdfunc=geodist(producedBy_samplingSite_location_ll,-17.47833,-149.92189)
        &fl=id,x:producedBy_samplingSite_location_longitude,y:producedBy_samplingSite_location_latitude,$gdfunc
        &sort=$gdfunc asc

    The full url (encoded) for 5000 records:

        /thing/stream?rows=5000&fl=id,x:producedBy_samplingSite_location_longitude,y:producedBy_samplingSite_location_latitude,$gdfunc&gdfunc=geodist%28producedBy_samplingSite_location_ll%2C-17.47833%2C-149.92189%29&sort=$gdfunc%20asc

    geodist(producedBy_samplingSite_location_ll,-17.47833,-149.92189)

    Example with curl:
    curl --data-urlencode \
      'expr=search(isb_core_records,q="source:SESAR",fq="searchText:sample",fq="hasMaterialCategory:Mineral",fl="id,producedBy_samplingSite_location_latitude,producedBy_samplingSite_location_longitude",sort="id asc",qt="/export")'\
      "http://localhost:8983/solr/isb_core_records/stream"

    For the ISB solr schema configuration, geodist() returns results in km.

    Args:
        params: list of [k,v], the parameters for the stream expression
        collection: name of collection to search

    Returns:
        Stream of records from solr
    """
    # TODO: Test coverage, need to mock solr?
    # TODO: C901 -- need to examine computational complexity

    point_rollup = False
    qparams: dict = {}
    url = get_solr_url("stream")
    headers = {"Accept": MEDIA_JSON}
    selection_method = "search"
    _params = []
    for kv in params:
        if kv[0] == "xycount":
            v = str(kv[1]).lower()
            if v in ["1", "true", "yes", "y"]:
                point_rollup = True
            else:
                point_rollup = False
            continue
        if kv[0] == "onlyxy":
            v = str(kv[1]).lower()
            if v in ["1", "true", "yes", "y"]:
                _params.append(
                    (
                        f'fq="{LONGITUDE_FIELD}:*'
                        f' AND {LATITUDE_FIELD}:*"'
                    )
                )
            continue
        if kv[0] == "select":
            assert kv[1] in ALLOWED_SELECT_METHODS
            selection_method = kv[1]
        if isinstance(kv[1], list):
            _params.append(f'{kv[0]}="{",".join(kv[1])}"')
        else:
            _params.append(f'{kv[0]}="{kv[1]}"')
    L.debug("_params = %s", _params)
    request = {
        "expr": f'{selection_method}({collection},{",".join(_params)},qt="/select")'
    }
    if point_rollup:
        request = {
            "expr": (
                f'select(rollup({selection_method}({collection},{",".join(_params)},qt="/select")'
                ',over="x,y",count(*)),x,y,count(*) as n)'
            )
        }
    L.info("Expression = %s", request["expr"])
    # Post the request to solr
    # The response is an open stream that is read in chunks to
    # be passed on to the client as they are received
    response = requests.post(
        url, headers=headers, params=qparams, data=request, stream=True
    )
    logging.info("Returning response")
    return response


def solr_luke():
    """
    Information about the solr isb_core_records schema
    See: https://solr.apache.org/guide/8_9/luke-request-handler.html

    Returns:
        JSON document iterator
    """
    url = get_solr_url("admin/luke")
    params = {"show": "schema", "wt": "json"}
    headers = {"Accept": MEDIA_JSON}
    response = requests.get(url, headers=headers, params=params, stream=True)
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=2048), media_type=MEDIA_JSON
    )


def _fetch_solr_records(
    rsession=requests.session(),
    authority_id: typing.Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 50000,
    field: typing.Optional[str] = None,
    sort: typing.Optional[str] = None,
    additional_query: typing.Optional[str] = None,
):
    headers = {"Content-Type": MEDIA_JSON}
    if additional_query is not None:
        if authority_id is not None:
            query = f"{additional_query} AND source:{authority_id}"
        else:
            query = additional_query
    elif authority_id is None:
        query = "*:*"
    else:
        query = f"source:{authority_id}"
    params = {
        "q": query,
        "rows": batch_size,
        "start": start_index,
    }
    if field is not None:
        params["fl"] = field
    if sort is not None:
        params["sort"] = sort
    _url = get_solr_url("select")
    res = rsession.get(_url, headers=headers, params=params)
    json = res.json()
    docs = json["response"]["docs"]
    num_found = json["response"]["numFound"]
    has_next = start_index + len(docs) < num_found
    return docs, has_next


def solr_records_for_sitemap(
    rsession=requests.session(),
    authority_id: typing.Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 50000,
) -> typing.List[typing.Dict]:
    """

    Args:
        rsession: The requests.session object to use for sending the solr request
        authority_id: The authority_id to use when querying SOLR, defaults to all
        start_index: The offset for the records to return
        batch_size: Number of documents for this particular sitemap document

    Returns:
        A list of dictionaries of solr documents with id and sourceUpdatedTime fields
    """
    return _fetch_solr_records(
        rsession,
        authority_id,
        start_index,
        batch_size,
        "id,sourceUpdatedTime",
        "sourceUpdatedTime asc",
    )[0]


def solr_records_for_stac_collection(
    authority_id: typing.Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 1000,
) -> Tuple[list[dict], bool]:
    """

    Args:
        authority_id: The authority_id to use when querying SOLR, defaults to all
        start_index: The offset for the records to return
        batch_size: Number of documents for this particular sitemap document

    Returns:
        A tuple of the dictionaries of solr documents with id and lat/lon fields, and whether there are more records
    """
    return _fetch_solr_records(
        requests.session(),
        authority_id,
        start_index,
        batch_size,
        "id,producedBy_samplingSite_location_longitude,producedBy_samplingSite_location_latitude,producedBy_resultTime,sourceUpdatedTime",
        "sourceUpdatedTime asc",
        "producedBy_samplingSite_location_longitude:* AND producedBy_samplingSite_location_latitude:*",
    )


def solr_records_forh3_counts(
    query: str, field_name: str, max_rows: int = -1
) -> dict:
    url = get_solr_url("stream")
    headers = {"Accept": MEDIA_JSON}
    dlm = ",\n"
    facet = (f'facet({DEFAULT_COLLECTION_NAME}{dlm}'
             f'q="{query}"{dlm}'
             f'buckets="{field_name}"{dlm}count(*),rows={max_rows})')
    response = requests.post(
        url, headers=headers, data={"expr": facet}, stream=True
    )
    logging.info("Returning response")
    return response.json()


def solr_counts_by_authority(rsession=requests.session()) -> dict[str, int]:
    url = get_solr_url("select")
    headers = {"Content-Type": MEDIA_JSON}
    params = {
        "q": "*:*",
        "facet": "true",
        "facet.field": "source",
        "facet.mincount": 1
    }
    res = rsession.get(url, headers=headers, params=params)
    json = res.json()
    facet_source_counts = json["facet_counts"]["facet_fields"]["source"]
    # The counts are a single array, with the string value followed by the count as the next item in the array, e.g.
    # "SESAR",100,"OPENCONTEXT",245…etc, so turn them into a nice dict instead.
    facet_counts_dict = {}
    for index, value in enumerate(facet_source_counts):
        if index % 2 != 0:
            continue
        facet_counts_dict[value] = facet_source_counts[index + 1]
    return facet_counts_dict


class ISBCoreSolrRecordIterator:
    """
    Iterator class for looping over all the Solr records in the ISB core Solr schema
    """

    def __init__(
        self,
        rsession=requests.session(),
        query: Optional[str] = None,
        batch_size: int = 50000,
        offset: int = 0,
        sort: Optional[str] = None,
    ):
        """

        Args:
            rsession: The requests.session object to use for sending the solr request
            authority_id: The authority_id to use when querying SOLR, defaults to all
            batch_size: Number of documents to fetch at a time
            offset: The offset into the records to begin iterating
            sort: The solr sort parameter to use
        """
        self.rsession = rsession
        self.query = query
        self.batch_size = batch_size
        self.offset = offset
        self.sort = sort
        self._current_batch: list[dict] = []
        self._current_batch_index = -1

    def __iter__(self):
        return self

    def __next__(self) -> typing.Dict:
        if len(self._current_batch) == 0 or self._current_batch_index == len(
            self._current_batch
        ):
            self._current_batch = _fetch_solr_records(
                self.rsession,
                None,
                self.offset,
                self.batch_size,
                None,
                self.sort,
                self.query
            )[0]
            if len(self._current_batch) == 0:
                # reached the end of the records
                raise StopIteration
            logging.info(
                f"Just fetched {len(self._current_batch)} ISB Core solr records at offset {self.offset}"
            )
            self.offset += self.batch_size
            self._current_batch_index = 0
        # return the next one in the list and increment our index
        next_record = self._current_batch[self._current_batch_index]
        self._current_batch_index = self._current_batch_index + 1
        return next_record
