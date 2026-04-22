
# -*- coding: utf-8 -*-
"""
Versão v3:
- municípios -> busca URL da prefeitura
- estados -> busca URL do governo do estado
- Distrito Federal / Brasília -> busca URL do GDF
- União -> busca portal do governo federal
- usa CNPJ para melhorar a decisão quando o nome é ambíguo
  (ex.: São Paulo / Rio de Janeiro podem ser estado ou município)

Pensado para rodar no Google Colab.
"""

import os
import re
import math
import time
import unicodedata
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# AJUSTE O CAMINHO SE PRECISAR
# =========================
INPUT_PATH = os.environ.get("INPUT_PATH", "input/base_contatos.csv")

OUTPUT_MAIN_CSV = os.environ.get("OUTPUT_MAIN_CSV", "output/base_contatos_municipais_com_urls_v3.csv")
OUTPUT_MAIN_XLSX = os.environ.get("OUTPUT_MAIN_XLSX", "output/base_contatos_municipais_com_urls_v3.xlsx")
OUTPUT_REVIEW_CSV = os.environ.get("OUTPUT_REVIEW_CSV", "output/revisao_urls_prefeituras_v3.csv")
OUTPUT_REVIEW_XLSX = os.environ.get("OUTPUT_REVIEW_XLSX", "output/revisao_urls_prefeituras_v3.xlsx")

MAX_WORKERS = 6
REQUEST_TIMEOUT = (8, 20)
SEARCH_PAUSE = 0.55
SAVE_EVERY = 25
MAX_SEARCH_RESULTS = 8
MIN_SCORE_TO_FILL = 40

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

STATE_TO_UF = {
    "acre": "AC",
    "alagoas": "AL",
    "amapa": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceara": "CE",
    "distrito federal": "DF",
    "espirito santo": "ES",
    "goias": "GO",
    "maranhao": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "para": "PA",
    "paraiba": "PB",
    "parana": "PR",
    "pernambuco": "PE",
    "piaui": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondonia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "sao paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}

REGIAO_TO_UFS = {
    "N":  ["AC", "AM", "AP", "PA", "RO", "RR", "TO"],
    "NE": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "CO": ["DF", "GO", "MT", "MS"],
    "SE": ["ES", "MG", "RJ", "SP"],
    "SU": ["PR", "RS", "SC"],
}

AMBIGUOUS_STATE_CITY = {"sao paulo", "rio de janeiro"}

EXCLUDED_DOMAINS = {
    "facebook.com", "www.facebook.com",
    "instagram.com", "www.instagram.com",
    "youtube.com", "www.youtube.com",
    "linkedin.com", "www.linkedin.com",
    "wikipedia.org", "pt.wikipedia.org",
    "jusbrasil.com.br", "www.jusbrasil.com.br",
    "govserv.org", "www.govserv.org",
    "prefeituras.org", "www.prefeituras.org",
    "cidade-brasil.com.br", "www.cidade-brasil.com.br",
    "citybrazil.com.br", "www.citybrazil.com.br",
    "guiamunicipios.com.br", "www.guiamunicipios.com.br",
    "transparencia.cc", "www.transparencia.cc",
}

html_cache = {}
alive_cache = {}


def normalize(text):
    if text is None:
        return ""
    text = str(text)
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    return text.lower().strip()


def slugify_name(text):
    text = normalize(text)
    text = re.sub(r"\b(de|da|do|das|dos|d')\b", " ", text)
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    parts = [p for p in text.split() if p]
    variants = []
    if parts:
        joined = "".join(parts)
        hyphen = "-".join(parts)
        for item in [joined, hyphen]:
            if item and item not in variants:
                variants.append(item)
    return variants


def only_digits(text):
    return re.sub(r"\D", "", "" if text is None else str(text))


def is_blank(v):
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return str(v).strip().lower() in {"", "nan", "none", "null", "n/a", "na", "-"}


def ensure_scheme(url):
    if is_blank(url):
        return ""
    url = str(url).strip()
    if not re.match(r"^https?://", url, flags=re.I):
        url = "https://" + url.lstrip("/")
    return url


def get_domain(url):
    try:
        return urlparse(ensure_scheme(url)).netloc.lower()
    except Exception:
        return ""


def get_root_url(url):
    try:
        p = urlparse(ensure_scheme(url))
        if not p.scheme or not p.netloc:
            return ""
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return ""


def domain_without_www(domain):
    return domain[4:] if domain.startswith("www.") else domain


def is_excluded_domain(domain):
    domain = domain_without_www(domain)
    if not domain:
        return True
    if domain in EXCLUDED_DOMAINS:
        return True
    return any(domain.endswith("." + bad) for bad in EXCLUDED_DOMAINS)


def create_session():
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


SESSION = create_session()


def clean_search_url(url):
    if not url:
        return ""
    url = url.strip()
    if "duckduckgo.com/l/?" in url:
        parsed = urlparse(url.replace("&amp;", "&"))
        qs = parse_qs(parsed.query)
        if "uddg" in qs and qs["uddg"]:
            return unquote(qs["uddg"][0])
    if url.startswith("//"):
        url = "https:" + url
    return url


def quick_alive(url):
    root = get_root_url(url) or ensure_scheme(url)
    if not root:
        return False

    if root in alive_cache:
        return alive_cache[root]

    ok = False
    try:
        r = SESSION.get(root, timeout=(6, 10), verify=False, allow_redirects=True)
        ctype = (r.headers.get("Content-Type") or "").lower()
        ok = r.status_code == 200 and "text/html" in ctype
    except requests.RequestException:
        ok = False

    alive_cache[root] = ok
    return ok


def fetch_html(url):
    root = get_root_url(url) or ensure_scheme(url)
    if not root:
        return ""

    if root in html_cache:
        return html_cache[root]

    html = ""
    try:
        r = SESSION.get(root, timeout=REQUEST_TIMEOUT, verify=False, allow_redirects=True)
        ctype = (r.headers.get("Content-Type") or "").lower()
        if r.status_code == 200 and "text/html" in ctype:
            html = r.text
    except requests.RequestException:
        html = ""

    html_cache[root] = html
    return html


def search_duckduckgo(query, max_results=MAX_SEARCH_RESULTS):
    url = "https://duckduckgo.com/html/"
    try:
        r = SESSION.get(url, params={"q": query}, timeout=REQUEST_TIMEOUT, verify=False)
        r.raise_for_status()
    except requests.RequestException:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select("a.result__a")
    snippets = soup.select(".result__snippet")

    rows = []
    for i, a in enumerate(links[:max_results]):
        href = clean_search_url(a.get("href", ""))
        title = a.get_text(" ", strip=True)
        snippet = snippets[i].get_text(" ", strip=True) if i < len(snippets) else ""
        if href:
            rows.append({
                "query": query,
                "title": title,
                "snippet": snippet,
                "url": href,
                "source": "duckduckgo",
            })
    return rows


def classify_entity(ente):
    ent = normalize(ente)

    if ent in {"uniao", "união"}:
        return "federal", None

    if ent in {"brasilia", "brasília", "distrito federal"}:
        return "district", "DF"

    if ent in STATE_TO_UF:
        uf = STATE_TO_UF[ent]
        if ent in AMBIGUOUS_STATE_CITY:
            return "ambiguous_state_or_city", uf
        return "state", uf

    return "municipality", None


def homepage_score(html, ente, expected_type, cnpj_digits):
    if not html:
        return -10, "homepage indisponível"

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    text = soup.get_text(" ", strip=True)[:50000]

    title_n = normalize(title)
    text_n = normalize(text)
    digits_n = only_digits(text)
    ente_n = normalize(ente)

    score = 0
    reasons = []

    if ente_n and ente_n in title_n:
        score += 10
        reasons.append("title menciona ente")
    if ente_n and ente_n in text_n:
        score += 12
        reasons.append("homepage menciona ente")

    if cnpj_digits and cnpj_digits in digits_n:
        score += 70
        reasons.append("homepage contém CNPJ da linha")

    if expected_type == "municipality":
        if "prefeitura municipal" in title_n or "prefeitura municipal" in text_n:
            score += 30
            reasons.append("indício forte de prefeitura")
        elif "prefeitura" in title_n or "prefeitura" in text_n:
            score += 18
            reasons.append("menciona prefeitura")
        if "governo do estado" in title_n or "governo do estado" in text_n:
            score -= 25
            reasons.append("parece governo estadual")

    elif expected_type == "state":
        if "governo do estado" in title_n or "governo do estado" in text_n:
            score += 28
            reasons.append("indício forte de governo estadual")
        elif "governo" in title_n or "governo" in text_n:
            score += 14
            reasons.append("menciona governo")
        if "prefeitura" in title_n or "prefeitura" in text_n:
            score -= 20
            reasons.append("parece prefeitura")

    elif expected_type == "district":
        if "governo do distrito federal" in title_n or "governo do distrito federal" in text_n:
            score += 28
            reasons.append("indício forte de GDF")
        elif "gdf" in title_n or "gdf" in text_n:
            score += 20
            reasons.append("menciona GDF")
        elif "distrito federal" in title_n or "distrito federal" in text_n:
            score += 12
            reasons.append("menciona Distrito Federal")

    elif expected_type == "federal":
        if "gov.br" in title_n or "gov.br" in text_n:
            score += 12
            reasons.append("menciona gov.br")
        if "governo federal" in title_n or "governo federal" in text_n:
            score += 20
            reasons.append("menciona governo federal")
        if "república" in title_n or "republica" in title_n:
            score += 8
            reasons.append("título institucional federal")

    return score, " | ".join(reasons[:6])


def score_candidate(ente, url, expected_type, cnpj_digits="", title="", snippet="", source="probe", uf_hint=""):
    root = get_root_url(url)
    domain = get_domain(root or url)
    if not domain:
        return -999, "url inválida", ""

    base_domain = domain_without_www(domain)
    if is_excluded_domain(base_domain):
        return -999, "domínio excluído", root

    title_n = normalize(title)
    snippet_n = normalize(snippet)
    combined = f"{title_n} {snippet_n}"
    ente_n = normalize(ente)

    score = 0
    reasons = [f"origem={source}", f"tipo={expected_type}"]

    if base_domain.endswith(".gov.br"):
        score += 32
        reasons.append("domínio gov.br")

    if expected_type == "municipality":
        if "prefeitura" in base_domain:
            score += 15
            reasons.append("domínio contém prefeitura")
        if uf_hint and f".{uf_hint.lower()}.gov.br" in base_domain:
            score += 10
            reasons.append("domínio contém UF")
        if "prefeitura municipal" in combined:
            score += 18
            reasons.append("snippet/título prefeitura municipal")
        elif "prefeitura" in combined:
            score += 10
            reasons.append("snippet/título prefeitura")
        if "governo do estado" in combined:
            score -= 20
            reasons.append("snippet parece governo estadual")

    elif expected_type == "state":
        if uf_hint and (base_domain == f"{uf_hint.lower()}.gov.br" or base_domain.endswith(f".{uf_hint.lower()}.gov.br")):
            score += 20
            reasons.append("domínio compatível com UF")
        if "governo do estado" in combined:
            score += 20
            reasons.append("snippet/título governo do estado")
        elif "governo" in combined:
            score += 10
            reasons.append("snippet/título governo")
        if "prefeitura" in combined:
            score -= 18
            reasons.append("snippet parece prefeitura")

    elif expected_type == "district":
        if "df.gov.br" in base_domain:
            score += 22
            reasons.append("domínio DF")
        if "gdf" in combined or "distrito federal" in combined:
            score += 16
            reasons.append("snippet/título GDF/DF")

    elif expected_type == "federal":
        if base_domain == "gov.br" or base_domain.endswith(".gov.br"):
            score += 16
            reasons.append("domínio gov.br")
        if "governo federal" in combined:
            score += 16
            reasons.append("snippet governo federal")

    html = fetch_html(root or url)
    h_score, h_obs = homepage_score(html, ente, expected_type, cnpj_digits)
    score += h_score
    if h_obs:
        reasons.append(h_obs)

    if ente_n and ente_n in combined:
        score += 8
        reasons.append("ente no título/snippet")

    return score, " | ".join(reasons[:7]), root


def direct_candidates_for_municipality(ente, regiao):
    slugs = slugify_name(ente)
    ufs = REGIAO_TO_UFS.get(str(regiao).strip().upper(), [])

    patterns = [
        "https://www.{slug}.{uf}.gov.br/",
        "https://{slug}.{uf}.gov.br/",
        "https://prefeitura.{slug}.{uf}.gov.br/",
        "https://www.prefeitura.{slug}.{uf}.gov.br/",
    ]

    rows = []
    for uf in ufs:
        for slug in slugs:
            for pat in patterns:
                rows.append({
                    "url": pat.format(slug=slug, uf=uf.lower()),
                    "title": "",
                    "snippet": "",
                    "query": f"probe_municipio:{slug}:{uf}",
                    "source": "direct_probe",
                    "candidate_type": "municipality",
                    "uf_hint": uf,
                })
    return rows


def direct_candidates_for_state(ente, uf):
    slugs = slugify_name(ente)
    rows = []

    patterns = [
        f"https://www.{uf.lower()}.gov.br/",
        f"https://{uf.lower()}.gov.br/",
    ]

    for slug in slugs:
        patterns.extend([
            f"https://www.{slug}.gov.br/",
            f"https://{slug}.gov.br/",
            f"https://www.governo.{slug}.gov.br/",
            f"https://governo.{slug}.gov.br/",
        ])

    for url in dict.fromkeys(patterns):
        rows.append({
            "url": url,
            "title": "",
            "snippet": "",
            "query": f"probe_estado:{uf}",
            "source": "direct_probe",
            "candidate_type": "state",
            "uf_hint": uf,
        })
    return rows


def direct_candidates_for_district():
    urls = [
        "https://www.df.gov.br/",
        "https://df.gov.br/",
    ]
    return [{
        "url": url,
        "title": "",
        "snippet": "",
        "query": "probe_df",
        "source": "direct_probe",
        "candidate_type": "district",
        "uf_hint": "DF",
    } for url in urls]


def direct_candidates_for_federal():
    urls = [
        "https://www.gov.br/",
        "https://gov.br/",
    ]
    return [{
        "url": url,
        "title": "",
        "snippet": "",
        "query": "probe_federal",
        "source": "direct_probe",
        "candidate_type": "federal",
        "uf_hint": "",
    } for url in urls]


def search_queries(entity_class, ente, regiao="", uf=""):
    queries = []

    if entity_class == "municipality":
        queries.extend([
            f'site:gov.br "Prefeitura Municipal de {ente}"',
            f'site:gov.br "{ente}" prefeitura municipal',
            f'site:gov.br "{ente}" prefeitura site oficial',
            f'"{ente}" "prefeitura" site:gov.br',
        ])
        for reg_uf in REGIAO_TO_UFS.get(str(regiao).strip().upper(), []):
            queries.append(f'site:gov.br "{ente}" "{reg_uf}" prefeitura')

    elif entity_class == "state":
        queries.extend([
            f'site:gov.br "Governo do Estado do {ente}"',
            f'site:gov.br "{ente}" governo do estado',
            f'site:gov.br "{ente}" governo estadual',
            f'site:gov.br "{uf}" governo',
        ])

    elif entity_class == "district":
        queries.extend([
            'site:gov.br "Governo do Distrito Federal"',
            'site:gov.br "Distrito Federal" governo',
            'site:gov.br GDF',
        ])

    elif entity_class == "federal":
        queries.extend([
            'site:gov.br "Governo Federal"',
            'site:gov.br "Portal Gov.br"',
            'site:gov.br "República Federativa do Brasil"',
        ])

    return queries


def search_candidates(entity_class, ente, regiao="", uf=""):
    rows = []
    seen = set()

    for q in search_queries(entity_class, ente, regiao, uf):
        found = search_duckduckgo(q)
        time.sleep(SEARCH_PAUSE)
        for row in found:
            root = get_root_url(row["url"])
            if root and root not in seen:
                seen.add(root)
                row["candidate_type"] = entity_class
                row["uf_hint"] = uf
                rows.append(row)

    return rows


def evaluate_candidates(ente, regiao, cnpj_digits, entity_class, uf):
    all_candidates = []

    if entity_class == "municipality":
        direct = direct_candidates_for_municipality(ente, regiao)
        search = search_candidates("municipality", ente, regiao, uf)
    elif entity_class == "state":
        direct = direct_candidates_for_state(ente, uf)
        search = search_candidates("state", ente, regiao, uf)
    elif entity_class == "district":
        direct = direct_candidates_for_district()
        search = search_candidates("district", ente, regiao, uf)
    else:
        direct = direct_candidates_for_federal()
        search = search_candidates("federal", ente, regiao, uf)

    for cand in direct:
        if quick_alive(cand["url"]):
            score, obs, root = score_candidate(
                ente=ente,
                url=cand["url"],
                expected_type=cand["candidate_type"],
                cnpj_digits=cnpj_digits,
                title=cand["title"],
                snippet=cand["snippet"],
                source=cand["source"],
                uf_hint=cand.get("uf_hint", ""),
            )
            if root:
                all_candidates.append({
                    "query": cand["query"],
                    "titulo": cand["title"],
                    "snippet": cand["snippet"],
                    "url_encontrada": cand["url"],
                    "melhor_url": root,
                    "score": score,
                    "confianca": classify(score),
                    "tipo_escolhido": cand["candidate_type"],
                    "metodo": "probe_direto",
                    "observacao": obs,
                })

    for cand in search:
        score, obs, root = score_candidate(
            ente=ente,
            url=cand["url"],
            expected_type=cand["candidate_type"],
            cnpj_digits=cnpj_digits,
            title=cand["title"],
            snippet=cand["snippet"],
            source=cand["source"],
            uf_hint=cand.get("uf_hint", ""),
        )
        if root:
            all_candidates.append({
                "query": cand["query"],
                "titulo": cand["title"],
                "snippet": cand["snippet"],
                "url_encontrada": cand["url"],
                "melhor_url": root,
                "score": score,
                "confianca": classify(score),
                "tipo_escolhido": cand["candidate_type"],
                "metodo": "busca_web",
                "observacao": obs,
            })

    dedup = {}
    for row in all_candidates:
        key = row["melhor_url"]
        if key not in dedup or row["score"] > dedup[key]["score"]:
            dedup[key] = row

    return sorted(dedup.values(), key=lambda x: x["score"], reverse=True)


def classify(score):
    if score >= 90:
        return "alta"
    if score >= 65:
        return "média"
    if score >= 40:
        return "baixa"
    return "revisar"


def load_table(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    if ext == ".csv":
        for enc in ["utf-8-sig", "utf-8", "latin1"]:
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception:
                continue

    raise ValueError("Não consegui ler o arquivo. Use CSV ou XLSX.")


def save_outputs(df_main, df_review):
    df_main.to_csv(OUTPUT_MAIN_CSV, index=False, encoding="utf-8-sig")
    df_main.to_excel(OUTPUT_MAIN_XLSX, index=False)
    df_review.to_csv(OUTPUT_REVIEW_CSV, index=False, encoding="utf-8-sig")
    df_review.to_excel(OUTPUT_REVIEW_XLSX, index=False)


def process_one(idx, ente, regiao, cnpj):
    ente = str(ente).strip()
    regiao = str(regiao).strip().upper()
    cnpj_digits = only_digits(cnpj)

    if not ente:
        return idx, "", {
            "linha": idx + 2,
            "ente": ente,
            "regiao": regiao,
            "cnpj": cnpj_digits,
            "melhor_url": "",
            "score": "",
            "confianca": "revisar",
            "status": "ente vazio",
            "tipo_identificado": "",
            "tipo_escolhido": "",
            "metodo": "",
            "observacao": "nome do ente não informado",
        }

    entity_class, uf = classify_entity(ente)

    # Casos ambíguos: tenta estado e município
    if entity_class == "ambiguous_state_or_city":
        ranked = []
        ranked.extend(evaluate_candidates(ente, regiao, cnpj_digits, "state", uf))
        ranked.extend(evaluate_candidates(ente, regiao, cnpj_digits, "municipality", uf))
        ranked = sorted(ranked, key=lambda x: x["score"], reverse=True)
        tipo_identificado = "estado_ou_municipio"
    else:
        ranked = evaluate_candidates(ente, regiao, cnpj_digits, entity_class, uf or "")
        tipo_identificado = entity_class

    if not ranked:
        return idx, "", {
            "linha": idx + 2,
            "ente": ente,
            "regiao": regiao,
            "cnpj": cnpj_digits,
            "melhor_url": "",
            "score": "",
            "confianca": "revisar",
            "status": "sem resultado",
            "tipo_identificado": tipo_identificado,
            "tipo_escolhido": "",
            "metodo": "",
            "observacao": "nenhum candidato encontrado",
        }

    best = ranked[0]
    chosen_url = best["melhor_url"] if best["score"] >= MIN_SCORE_TO_FILL else ""

    review_row = {
        "linha": idx + 2,
        "ente": ente,
        "regiao": regiao,
        "cnpj": cnpj_digits,
        "melhor_url": best["melhor_url"],
        "score": best["score"],
        "confianca": best["confianca"],
        "status": "preenchido" if chosen_url else "revisar manualmente",
        "tipo_identificado": tipo_identificado,
        "tipo_escolhido": best["tipo_escolhido"],
        "metodo": best["metodo"],
        "observacao": best["observacao"],
    }

    return idx, chosen_url, review_row


def main():
    os.makedirs(os.path.dirname(OUTPUT_MAIN_CSV) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_MAIN_XLSX) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_REVIEW_CSV) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_REVIEW_XLSX) or '.', exist_ok=True)
    df = load_table(INPUT_PATH)

    for col in ["ente", "regiao", "cnpj"]:
        if col not in df.columns:
            raise ValueError(f'A coluna obrigatória "{col}" não foi encontrada.')

    if "URL Prefeitura" not in df.columns:
        df["URL Prefeitura"] = ""

    df["URL Prefeitura"] = df["URL Prefeitura"].astype("string").fillna("")
    df["ente"] = df["ente"].astype(str).str.strip()
    df["regiao"] = df["regiao"].astype(str).str.strip().str.upper()
    df["cnpj"] = df["cnpj"].astype(str).str.strip()

    pending_idx = df.index[df["URL Prefeitura"].str.strip().eq("")].tolist()
    review_rows = []

    print(f"Total de linhas: {len(df)}")
    print(f"Linhas para buscar: {len(pending_idx)}")

    workers = min(MAX_WORKERS, max(1, len(pending_idx)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(process_one, idx, df.at[idx, "ente"], df.at[idx, "regiao"], df.at[idx, "cnpj"]): idx
            for idx in pending_idx
        }

        total = len(futures)
        for i, future in enumerate(as_completed(futures), start=1):
            idx, found_url, review_row = future.result()

            if found_url:
                df.at[idx, "URL Prefeitura"] = found_url

            review_rows.append(review_row)

            if i % SAVE_EVERY == 0 or i == total:
                df_review = pd.DataFrame(review_rows).sort_values(by=["status", "ente"], na_position="last")
                save_outputs(df, df_review)

            status_text = found_url if found_url else review_row["status"].upper()
            print(f"[{i}/{total}] {df.at[idx, 'ente']} -> {status_text}")

    df_review = pd.DataFrame(review_rows).sort_values(by=["status", "ente"], na_position="last")
    save_outputs(df, df_review)

    print("\nConcluído.")
    print(f"Arquivo principal CSV: {OUTPUT_MAIN_CSV}")
    print(f"Arquivo principal XLSX: {OUTPUT_MAIN_XLSX}")
    print(f"Arquivo revisão CSV: {OUTPUT_REVIEW_CSV}")
    print(f"Arquivo revisão XLSX: {OUTPUT_REVIEW_XLSX}")


if __name__ == "__main__":
    main()
