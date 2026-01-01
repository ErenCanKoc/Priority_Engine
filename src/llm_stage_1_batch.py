# src/llm_stage_1_batch.py
import os
import json
import time
import logging
import pandas as pd
from datetime import datetime
from openai import OpenAI

from config import OUTPUT_DIR, OPENAI_API_KEY
from llm_prompts import TRIAGE_SYSTEM_PROMPT

# Model adı düzeltildi (gpt-4.1-mini geçersizdi)
MODEL = "gpt-4o-mini"
BATCH_CHECK_INTERVAL = 60  # saniye

client = OpenAI(api_key=OPENAI_API_KEY)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'llm_batch.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _build_user_input(r):
    """Batch için user input oluşturur (triage ile aynı format)."""
    return f"""
URL: {r.get('url','')}
Query: {r.get('keyword','')}

Analyze candidate: {r.get('analyze_candidate','')}
Candidate type: {r.get('candidate_type','')}
Candidate reason: {r.get('candidate_reason','')}
Engine status: {r.get('engine_status','')}

Page type: {r.get('page_type','')}
Query type: {r.get('query_type','')}
Problem type: {r.get('problem_type','')}

Clicks (current): {r.get('clicks_last','')}
Clicks (previous): {r.get('clicks_prev','')}
Impressions (current): {r.get('impr_last','')}
Impressions (previous): {r.get('impr_prev','')}
Avg position: {r.get('pos','')}

Estimated MSV (monthly): {r.get('msv_est','')}
Utilization ratio: {r.get('utilization','')}
Traffic gap (clicks): {r.get('traffic_gap','')}

SERP status: {r.get('serp_status','')}
SERP features: {r.get('serp_features','')}
SERP rank: {r.get('serp_rank','')}
SERP competition: {r.get('serp_competition','')}
SERP error: {r.get('serp_error','')}
""".strip()


def build_batch_requests():
    """
    Batch API için JSONL input dosyası oluşturur.
    Returns: JSONL dosya yolu
    """
    in_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df = pd.read_csv(in_path)

    # Sadece analyze_candidate=True olanlar
    candidates = df[df["analyze_candidate"] == True].copy()
    
    logger.info(f"Batch input oluşturuluyor: {len(candidates)} satır")

    requests = []
    for idx, r in candidates.iterrows():
        user_input = _build_user_input(r)

        body = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_input},
            ],
            "temperature": 0.2,
        }

        requests.append({
            "custom_id": f"row_{idx}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body,
        })

    out_jsonl = os.path.join(OUTPUT_DIR, "llm_stage_1_batch_input.jsonl")
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for r in requests:
            f.write(json.dumps(r) + "\n")

    logger.info(f"✓ Batch input oluşturuldu → {out_jsonl}")
    return out_jsonl


def upload_batch_file(jsonl_path: str) -> str:
    """
    JSONL dosyasını OpenAI'a yükler.
    Returns: file_id
    """
    logger.info(f"Dosya yükleniyor: {jsonl_path}")
    
    with open(jsonl_path, "rb") as f:
        file_response = client.files.create(
            file=f,
            purpose="batch"
        )
    
    file_id = file_response.id
    logger.info(f"✓ Dosya yüklendi: {file_id}")
    return file_id


def create_batch_job(file_id: str) -> str:
    """
    Batch job oluşturur.
    Returns: batch_id
    """
    logger.info(f"Batch job oluşturuluyor, file_id: {file_id}")
    
    batch_response = client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={
            "description": "SEO Pipeline Stage 1 Triage",
            "created_at": datetime.now().isoformat()
        }
    )
    
    batch_id = batch_response.id
    logger.info(f"✓ Batch job oluşturuldu: {batch_id}")
    
    # Batch ID'yi dosyaya kaydet (resume için)
    batch_info = {
        "batch_id": batch_id,
        "file_id": file_id,
        "created_at": datetime.now().isoformat(),
        "status": "created"
    }
    info_path = os.path.join(OUTPUT_DIR, "batch_job_info.json")
    with open(info_path, "w") as f:
        json.dump(batch_info, f, indent=2)
    
    return batch_id


def check_batch_status(batch_id: str) -> dict:
    """Batch job durumunu kontrol eder."""
    batch = client.batches.retrieve(batch_id)
    
    return {
        "id": batch.id,
        "status": batch.status,
        "created_at": batch.created_at,
        "completed_at": batch.completed_at,
        "failed_at": batch.failed_at,
        "request_counts": {
            "total": batch.request_counts.total,
            "completed": batch.request_counts.completed,
            "failed": batch.request_counts.failed
        },
        "output_file_id": batch.output_file_id,
        "error_file_id": batch.error_file_id
    }


def wait_for_batch_completion(batch_id: str, max_wait_hours: int = 24) -> dict:
    """
    Batch tamamlanana kadar bekler.
    Returns: Final batch status
    """
    max_iterations = (max_wait_hours * 3600) // BATCH_CHECK_INTERVAL
    
    for i in range(max_iterations):
        status = check_batch_status(batch_id)
        
        logger.info(
            f"Batch durumu: {status['status']} | "
            f"Tamamlanan: {status['request_counts']['completed']}/{status['request_counts']['total']} | "
            f"Başarısız: {status['request_counts']['failed']}"
        )
        
        if status["status"] == "completed":
            logger.info("✓ Batch tamamlandı!")
            return status
        
        if status["status"] in ["failed", "expired", "cancelled"]:
            logger.error(f"✗ Batch başarısız: {status['status']}")
            return status
        
        time.sleep(BATCH_CHECK_INTERVAL)
    
    logger.warning(f"⚠ Maksimum bekleme süresi aşıldı ({max_wait_hours} saat)")
    return check_batch_status(batch_id)


def download_batch_results(output_file_id: str) -> str:
    """
    Batch sonuçlarını indirir.
    Returns: İndirilen dosya yolu
    """
    logger.info(f"Sonuçlar indiriliyor: {output_file_id}")
    
    content = client.files.content(output_file_id)
    
    out_path = os.path.join(OUTPUT_DIR, "llm_stage_1_batch_output.jsonl")
    with open(out_path, "wb") as f:
        f.write(content.read())
    
    logger.info(f"✓ Sonuçlar indirildi → {out_path}")
    return out_path


def parse_batch_results(results_path: str):
    """
    Batch sonuçlarını parse edip ana CSV'ye yazar.
    """
    logger.info(f"Batch sonuçları parse ediliyor: {results_path}")
    
    # Ana veriyi yükle
    in_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df = pd.read_csv(in_path)
    
    # Output kolonlarını garantile
    for c in [
        "llm_stage_1_status",
        "llm_stage_1_verdict",
        "llm_stage_1_confidence",
        "llm_stage_1_problem",
        "llm_stage_1_cause",
        "llm_stage_1_opportunity",
        "llm_stage_1_evidence",
        "llm_stage_1_reasoning_raw",
        "llm_stage_1_risk_flags",
    ]:
        if c not in df.columns:
            df[c] = ""
    
    # Defaults
    df["llm_stage_1_status"] = "skipped"
    df["llm_stage_1_verdict"] = "ignore"
    df["llm_stage_1_confidence"] = "low"
    
    # Batch sonuçlarını oku
    results = {}
    with open(results_path, "r", encoding="utf-8") as f:
        for line in f:
            result = json.loads(line)
            custom_id = result.get("custom_id", "")
            results[custom_id] = result
    
    logger.info(f"Toplam {len(results)} batch sonucu okundu")
    
    # Metrikleri takip et
    success_count = 0
    error_count = 0
    
    for custom_id, result in results.items():
        try:
            # custom_id formatı: "row_123"
            idx = int(custom_id.replace("row_", ""))
            
            # Response'u parse et
            response = result.get("response", {})
            
            if response.get("status_code") != 200:
                df.loc[idx, "llm_stage_1_status"] = "error"
                df.loc[idx, "llm_stage_1_risk_flags"] = f"api_error:{response.get('status_code')}"
                error_count += 1
                continue
            
            # Body'den content'i al
            body = response.get("body", {})
            choices = body.get("choices", [])
            
            if not choices:
                df.loc[idx, "llm_stage_1_status"] = "error"
                df.loc[idx, "llm_stage_1_risk_flags"] = "empty_response"
                error_count += 1
                continue
            
            raw = choices[0].get("message", {}).get("content", "").strip()
            
            # JSON parse
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                df.loc[idx, "llm_stage_1_status"] = "error"
                df.loc[idx, "llm_stage_1_verdict"] = "monitor"
                df.loc[idx, "llm_stage_1_confidence"] = "low"
                df.loc[idx, "llm_stage_1_reasoning_raw"] = raw[:500]
                df.loc[idx, "llm_stage_1_risk_flags"] = "json_parse_failed"
                error_count += 1
                continue
            
            # Değerleri extract et
            verdict = parsed.get("verdict", "monitor")
            confidence = parsed.get("confidence", "low")
            reasoning = parsed.get("reasoning", {})
            risk_flags = parsed.get("risk_flags", [])
            
            # Structured reasoning parse
            if isinstance(reasoning, dict):
                problem = reasoning.get("problem", "")
                cause = reasoning.get("cause", "")
                opportunity = reasoning.get("opportunity", "")
                evidence = reasoning.get("evidence", "")
            elif isinstance(reasoning, str):
                problem = reasoning[:200]
                cause = ""
                opportunity = ""
                evidence = ""
                risk_flags = list(set(risk_flags + ["reasoning_format_error"]))
            else:
                problem = ""
                cause = ""
                opportunity = ""
                evidence = ""
            
            # SERP kontrolü
            if df.loc[idx, "serp_status"] != "ok":
                confidence = "low"
                risk_flags = list(set(risk_flags + ["serp_unavailable"]))
            
            # Değerleri yaz
            df.loc[idx, "llm_stage_1_status"] = "ok"
            df.loc[idx, "llm_stage_1_verdict"] = verdict
            df.loc[idx, "llm_stage_1_confidence"] = confidence
            df.loc[idx, "llm_stage_1_problem"] = problem
            df.loc[idx, "llm_stage_1_cause"] = cause
            df.loc[idx, "llm_stage_1_opportunity"] = opportunity
            df.loc[idx, "llm_stage_1_evidence"] = evidence
            df.loc[idx, "llm_stage_1_reasoning_raw"] = json.dumps(reasoning) if reasoning else ""
            df.loc[idx, "llm_stage_1_risk_flags"] = "; ".join(risk_flags) if risk_flags else ""
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"Satır {custom_id} işlenirken hata: {str(e)}")
            error_count += 1
    
    # Kaydet
    out_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df.to_csv(out_path, index=False)
    
    logger.info(f"✓ Batch sonuçları işlendi → {out_path}")
    logger.info(f"  Başarılı: {success_count}, Hatalı: {error_count}")
    
    return out_path


def run_llm_stage_1_batch(wait_for_completion: bool = True):
    """
    Tam batch pipeline: upload → create job → wait → download → parse
    
    Args:
        wait_for_completion: True ise batch bitene kadar bekler,
                            False ise sadece job oluşturur ve döner
    """
    logger.info("=" * 60)
    logger.info("LLM STAGE 1 - BATCH PROCESSING")
    logger.info("=" * 60)
    
    # 1. Batch input oluştur
    jsonl_path = build_batch_requests()
    
    # 2. Dosyayı yükle
    file_id = upload_batch_file(jsonl_path)
    
    # 3. Batch job oluştur
    batch_id = create_batch_job(file_id)
    
    if not wait_for_completion:
        logger.info(f"Batch job oluşturuldu: {batch_id}")
        logger.info("Durumu kontrol etmek için: check_batch_status(batch_id)")
        return batch_id
    
    # 4. Tamamlanmasını bekle
    final_status = wait_for_batch_completion(batch_id)
    
    if final_status["status"] != "completed":
        logger.error(f"Batch başarısız oldu: {final_status['status']}")
        return None
    
    # 5. Sonuçları indir
    results_path = download_batch_results(final_status["output_file_id"])
    
    # 6. Parse et ve CSV'ye yaz
    output_path = parse_batch_results(results_path)
    
    logger.info("✓ Batch processing tamamlandı!")
    return output_path


def resume_batch_processing():
    """
    Daha önce oluşturulmuş batch job'ı devam ettirir.
    batch_job_info.json dosyasından batch_id okur.
    """
    info_path = os.path.join(OUTPUT_DIR, "batch_job_info.json")
    
    if not os.path.exists(info_path):
        logger.error(f"Batch info dosyası bulunamadı: {info_path}")
        return None
    
    with open(info_path, "r") as f:
        info = json.load(f)
    
    batch_id = info.get("batch_id")
    logger.info(f"Batch job devam ettiriliyor: {batch_id}")
    
    # Durumu kontrol et
    status = check_batch_status(batch_id)
    
    if status["status"] == "completed":
        # Zaten tamamlanmış, sonuçları indir
        results_path = download_batch_results(status["output_file_id"])
        return parse_batch_results(results_path)
    
    if status["status"] in ["failed", "expired", "cancelled"]:
        logger.error(f"Batch başarısız: {status['status']}")
        return None
    
    # Hala çalışıyor, bekle
    final_status = wait_for_batch_completion(batch_id)
    
    if final_status["status"] == "completed":
        results_path = download_batch_results(final_status["output_file_id"])
        return parse_batch_results(results_path)
    
    return None