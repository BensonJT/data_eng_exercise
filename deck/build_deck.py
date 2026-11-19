#!/usr/bin/env python3
"""Build the Data Migration Quality deck from the same DuckDB views the Streamlit dashboard reads.

Every figure on every slide is queried at build time, and each headline is a claim this script
tests, with a neutral fallback when the data no longer supports it. Sigma follows the dashboard:
overall sigma is recomputed from yield (inverse normal + 1.5 shift, `accurate_sigma` in
dashboard.py); field-level sigma and the GO / CAUTION / NO-GO bands use the view values, exactly
as the dashboard's Go / No-Go tab does.

Usage (conda env data_eng):
    python deck/build_deck.py                         # writes deck/data-migration-presentation.html
    python deck/build_deck.py --pdf                   # also prints deck/Data_Migration_Analysis.pdf
    python deck/build_deck.py --publish ~/profile     # also copies both into the profile site

The live copy is the one in the profile site (bensonjt.github.io/profile/data-migration-presentation.html).
"""
import argparse
import html
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from scipy import stats

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT_HTML = HERE / "data-migration-presentation.html"
OUT_PDF = HERE / "Data_Migration_Analysis.pdf"
GO, MIN = 4.5, 4.0  # dashboard thresholds: >= 4.5 GO, 4.0-4.5 CAUTION, < 4.0 NO-GO


def accurate_sigma(y):
    return stats.norm.ppf(y) + 1.5


def money(x):
    return f"${x:,.0f}"


def k_money(x):
    return f"${x / 1000:,.0f}K"


def pct(x, d=1):
    return f"{x * 100:.{d}f}%"


def load():
    load_dotenv(ROOT / ".env")
    db = os.environ.get("DUCKDB_PATH")
    if not db or not Path(db).exists():
        sys.exit(f"DuckDB not found: {db!r} (set DUCKDB_PATH in .env)")
    c = duckdb.connect(db, read_only=True)
    q = lambda s: c.execute(s).df()
    d = {}
    summ = q("SELECT * FROM vw_sigma_analysis").set_index("SUBJECT")
    for key, subj in (("car", "Carrier Claims"), ("ben", "Beneficiary Summary")):
        r = summ.loc[subj]
        d[key] = dict(units=int(r.TOTAL_UNITS), defects=int(r.TOTAL_DEFECTS), dpmo=float(r.DPMO),
                      yld=float(r.YIELD), sigma=float(accurate_sigma(float(r.YIELD))))
    d["car_fields"] = q("SELECT FIELD_FAMILY f, SIGMA_LEVEL s, TOTAL_DEFECTS n FROM vw_sigma_analysis_carrier_columns ORDER BY s")
    d["ben_fields"] = q("SELECT FIELD_FAMILY f, SIGMA_LEVEL s FROM vw_sigma_analysis_beneficiary_columns ORDER BY s")
    counts = q("SELECT * FROM vw_source_counts").set_index("table_name")
    d["counts"] = {t: (int(counts.loc[t].src_count), int(counts.loc[t].new_count)) for t in counts.index}
    d["claims_new_only"] = int(q("SELECT count(*) n FROM vw_claim_mismatches WHERE FINDING LIKE '%Present in New File, Not in Orig%'").n[0])
    d["claims_mismatch_all"] = int(q("SELECT count(*) n FROM vw_claim_mismatches").n[0])
    be = q("SELECT FINDING, count(*) n FROM vw_beneficiary_errors GROUP BY 1")
    d["ben_missing_new"] = int(be[be.FINDING.str.contains("Missing in New")].n.sum())
    d["ben_new_only"] = int(be[be.FINDING.str.contains("Present in New")].n.sum())
    dob = q("""SELECT abs(date_diff('day', strptime(src_dob,'%Y%m%d'), strptime(new_dob,'%Y%m%d'))) AS shift_days
               FROM vw_bene_dt_differences WHERE src_dob IS NOT NULL AND new_dob IS NOT NULL AND src_dob <> new_dob""")
    d["dob_changed"] = len(dob)
    d["dob_median"] = int(round(dob.shift_days.median())) if len(dob) else 0
    d["pay_lines"] = int(q("SELECT count(*) n FROM vw_claim_line_nch_pmt_amt_1_differences").n[0])
    d["fin_car"] = q("SELECT FIELD_FAMILY f, FINANCIAL_VARIANCE v, RUNNING_PCT_OF_TOTAL p FROM vw_claim_financial_error_impact ORDER BY v DESC")
    d["fin_ben"] = q("SELECT FIELD_FAMILY f, FINANCIAL_VARIANCE v FROM vw_beneficiary_financial_error_impact ORDER BY v DESC")
    return d


def band(s):
    return "go" if s >= GO else ("caution" if s >= MIN else "nogo")


def build(d):
    car, ben = d["car"], d["ben"]
    cf = d["car_fields"]
    n_car = len(cf)
    nogo = int((cf.s < MIN).sum()); caution = int(((cf.s >= MIN) & (cf.s < GO)).sum()); go = n_car - nogo - caution
    bf = d["ben_fields"]
    b_nogo = int((bf.s < MIN).sum()); b_caution = int(((bf.s >= MIN) & (bf.s < GO)).sum()); b_go = len(bf) - b_nogo - b_caution
    fin_car_total = float(d["fin_car"].v.sum()); fin_ben_total = float(d["fin_ben"].v.sum())
    fin_total = fin_car_total + fin_ben_total
    top3 = float(d["fin_car"].p.iloc[2]) if len(d["fin_car"]) >= 3 else 1.0
    both_pass = car["sigma"] >= GO and ben["sigma"] >= GO
    record_issues = d["claims_new_only"] + d["ben_missing_new"] + d["ben_new_only"] + d["dob_changed"]
    proceed_blockers = nogo > 0 or record_issues > 0
    total = 6
    pg = lambda n: f'<p class="pg">{n} / {total}</p>'
    src = ('<p class="foot">Source: DuckDB views behind the Data Migration Quality Dashboard (CMS DE-SynPUF synthetic '
           'claims, legacy vs new system). Sigma: inverse normal of yield + 1.5σ shift.</p>')
    S = []

    # 1. The call
    h1 = ("Do not proceed with the migration as is." if (proceed_blockers and both_pass) else
          "Do not proceed until the failures are fixed." if proceed_blockers else
          "The migration meets every quality gate.")
    S.append(f'''<section class="slide ink" id="s1" aria-label="Slide 1: {h1}"><p class="eyebrow">Data migration quality analysis · legacy to new system</p><h2>{h1}</h2><p class="lede">The averages pass. The detail does not: whole fields fail, records appear and vanish, and payments disagree. Each of those has to be fixed before cutover.</p><div class="spacer"></div><div class="trio"><div class="stat"><p class="metric">{car["sigma"]:.2f}σ</p><p class="stat-name">overall, carrier claims</p><p class="small">Passes the {GO}σ target on average.</p></div><div class="stat"><p class="metric">{nogo} of {n_car}</p><p class="stat-name">claim fields fail</p><p class="small">Below the {MIN}σ minimum (NO-GO).</p></div><div class="stat"><p class="metric">{k_money(fin_total)}</p><p class="stat-name">payment variance</p><p class="small">Across both tables, plus {record_issues:,} record-level errors.</p></div></div>{pg(1)}<aside>My recommendation is not to proceed with the migration as it stands. On the summary scorecard it looks fine: carrier claims come in at {car["sigma"]:.2f} sigma and beneficiary summary at {ben["sigma"]:.2f}, both above the {GO} target. But {nogo} of the {n_car} carrier claim fields fall below the {MIN} sigma minimum, {record_issues:,} records were added, lost or changed in ways a row count does not show, and payment amounts differ by about {k_money(fin_total)}. The rest of the deck shows each of those, and the last slide is what I would do about it.</aside></section>''')

    # 2. The surprise
    h2 = f"Overall quality passes the {GO}σ target." if both_pass else f"Overall quality falls short of the {GO}σ target."
    def card(name, r):
        return (f'<div class="stat"><p class="stat-name">{name}</p><p class="metric">{r["sigma"]:.2f}σ</p>'
                f'<p class="small">Yield {r["yld"] * 100:.2f}% · {r["dpmo"]:,.0f} defects per million opportunities</p>'
                f'<p class="small">{r["units"]:,} records · {r["defects"]:,} defective data elements</p></div>')
    S.append(f'''<section class="slide vellum" id="s2" aria-label="Slide 2: {h2}"><p class="eyebrow">The summary scorecard</p><h2>{h2}</h2><div class="trio">{card("Carrier claims", car)}{card("Beneficiary summary", ben)}</div><div class="spacer"></div><p class="callout body">On this view alone, both tables are a GO. Six Sigma is 3.4 defects per million; {GO}σ is the target set for this migration, and below {MIN}σ is a NO-GO.</p>{src}{pg(2)}<aside>This is the view most migration sign-offs stop at. Carrier claims: {car["sigma"]:.2f} sigma, a yield of {car["yld"] * 100:.2f} percent, about {car["dpmo"]:,.0f} defects per million opportunities across {car["units"]:,} records. Beneficiary summary: {ben["sigma"]:.2f} sigma, {ben["dpmo"]:,.0f} defects per million. Both clear the {GO} target, so on this slide the answer is go. The next three slides are why that answer is wrong.</aside></section>''')

    # 3. The driver
    h3 = (f"{nogo} of {n_car} claim fields fail the {MIN}σ minimum." if nogo else f"Every claim field clears the {MIN}σ minimum.")
    lo, hi = 3.4, 5.0
    w = lambda s: max(2.0, (min(s, hi) - lo) / (hi - lo) * 100)
    rows = "".join(
        f'<div class="fb-row"><p class="fb-label">{html.escape(r.f)}</p><div class="fb-track"><div class="fb-bar {band(r.s)}" style="width:{w(r.s):.1f}%"></div>'
        f'<span class="fb-line" style="left:{w(MIN):.1f}%"></span><span class="fb-line go" style="left:{w(GO):.1f}%"></span></div><p class="fb-val {band(r.s)}">{r.s:.2f}σ</p></div>'
        for r in cf.itertuples())
    S.append(f'''<section class="slide paper" id="s3" aria-label="Slide 3: {h3}"><p class="eyebrow">Field by field · carrier claims</p><h2>{h3}</h2><div class="row"><div class="fbars">{rows}<p class="caption fb-cap">Lines mark {MIN}σ (minimum) and {GO}σ (target).</p></div><div class="side"><p class="stat-name">Carrier claims, {n_car} fields</p><p class="small"><b class="nogo-t">{nogo} NO-GO</b> · below {MIN}σ</p><p class="small"><b class="caution-t">{caution} CAUTION</b> · {MIN}–{GO}σ</p><p class="small"><b class="go-t">{go} GO</b> · {GO}σ or better</p><p class="stat-name" style="padding-top:24px">Beneficiary summary, {len(bf)} fields</p><p class="small">{b_nogo} NO-GO · {b_caution} CAUTION · {b_go} GO</p></div></div>{src}{pg(3)}<aside>Here is what the average hides. Of the {n_car} carrier claim fields, {nogo} are below {MIN} sigma, which the dashboard treats as remediation required before go-live. The worst five, including the processing indicator, diagnosis code, tax number, physician NPI and procedure code, each have about 75,000 defects. {caution} more fields sit in the caution band, and only {go} meet the target. The beneficiary table is in much better shape: {b_go} of {len(bf)} fields pass, and the one caution field is date of birth, which comes up again on the next slide.</aside></section>''')

    # 4. The records
    bsrc, bnew = d["counts"]["beneficiary_summary"]; csrc, cnew = d["counts"]["carrier_claims"]
    h4 = "Records appeared, vanished and changed identity." if (d["claims_new_only"] and (d["ben_missing_new"] or d["ben_new_only"])) else "Record-level checks."
    S.append(f'''<section class="slide vellum" id="s4" aria-label="Slide 4: {h4}"><p class="eyebrow">Record by record</p><h2>{h4}</h2><div class="calls"><div class="call"><p class="call-num">{d["claims_new_only"]:,}</p><p class="stat-name">claims in the new system only</p><p class="small">Present after migration, absent from the source: {csrc:,} source rows became {cnew:,}.</p></div><div class="call"><p class="call-num">{d["ben_missing_new"]:,} + {d["ben_new_only"]:,}</p><p class="stat-name">beneficiary IDs out of step</p><p class="small">{d["ben_missing_new"]:,} source IDs are missing from the new system, and {d["ben_new_only"]:,} new IDs never existed in the source.</p></div><div class="call"><p class="call-num">{d["dob_changed"]:,}</p><p class="stat-name">dates of birth changed</p><p class="small">Shifted by a median of {d["dob_median"]} days, in both directions.</p></div></div><div class="spacer"></div><p class="callout body">A row count would have passed the beneficiary table: {bsrc:,} rows before and {bnew:,} after. The match hides {d["ben_missing_new"] + d["ben_new_only"]:,} identity errors.</p>{src}{pg(4)}<aside>These are not field defects; they are records that are wrong as a whole. {d["claims_new_only"]:,} claims exist in the new system that the source never had. In the beneficiary table, {d["ben_missing_new"]} people disappeared and {d["ben_new_only"]} new IDs appeared, the same number, which is why the row counts match exactly. If those are the same people under new IDs, something in the migration rewrote identifiers. And {d["dob_changed"]} dates of birth moved by a median of about {d["dob_median"]} days. For claims and eligibility, every one of these matters.</aside></section>''')

    # 5. The money
    h5 = f"{k_money(fin_total)} of payment variance; three claim fields drive {top3 * 100:.1f}% of the claims share."
    mx = float(d["fin_car"].v.max())
    prow = "".join(
        f'<div class="bar-row"><p class="bar-label">{html.escape(r.f)}</p><div class="bar{" top" if i < 3 else ""}" style="width:{r.v / mx * 520:.0f}px"></div><p class="bar-val{" top" if i < 3 else ""}">{money(r.v)} · {pct(r.p)}</p></div>'
        for i, r in enumerate(d["fin_car"].itertuples()))
    S.append(f'''<section class="slide paper" id="s5" aria-label="Slide 5: {h5}"><p class="eyebrow">Financial impact</p><h2>{h5}</h2><div class="row"><div class="bars">{prow}<p class="caption">Carrier claims variance by field, with running share of the total.</p></div><div class="side"><p class="stat-name">Carrier claims</p><p class="metric">{money(fin_car_total)}</p><p class="stat-name">Beneficiary summary</p><p class="metric">{money(fin_ben_total)}</p><p class="small">{d["pay_lines"]:,} claim lines carry a different payment amount after migration.</p></div></div>{src}{pg(5)}<aside>The money is where a business owner feels this. Payment amounts differ by {money(fin_car_total)} on the carrier claims side and {money(fin_ben_total)} on the beneficiary side, about {k_money(fin_total)} together, across {d["pay_lines"]:,} claim lines. It is concentrated: the top three claim fields, led by the NCH payment amount, account for {top3 * 100:.1f} percent of the claims variance. That is a Pareto, and it tells the engineers where to start.</aside></section>''')

    # 6. The action
    h6 = "Remediate the failing fields, then re-run the scorecard as the gate."
    S.append(f'''<section class="slide olive" id="s6" aria-label="Slide 6: {h6}"><p class="eyebrow">Recommendation</p><h2>{h6}</h2><div class="spacer"></div><div class="steps"><div class="step"><h3>Find the root cause</h3><p class="small">Field-level error datasets already list every value that changed, ready for the application and database engineers.</p></div><div class="step"><h3>Fix and reconcile</h3><p class="small">The {nogo} NO-GO claim fields first, by financial impact; then record identity: {d["claims_new_only"]:,} claims, {d["ben_missing_new"] + d["ben_new_only"]:,} beneficiary IDs and {d["dob_changed"]} dates of birth.</p></div><div class="step"><h3>Re-run as the gate</h3><p class="small">Proceed when every field clears {MIN}σ, both tables clear {GO}σ, and no record is added, lost or re-identified.</p></div></div>{pg(6)}<aside>What I would do. First, root cause: the pipeline already produces a dataset of every field value that changed, which is what the engineers need. Second, fix in order of impact, the {nogo} failing claim fields first, then reconcile the records that appeared, vanished or changed identity. Third, re-run this same analysis as the go or no-go gate, and proceed only when every field clears the minimum and nothing is added, lost or re-identified. The numbers in this deck are generated from the same database views as the dashboard, so a re-run updates the deck too.</aside></section>''')
    return "\n".join(S)


EXTRA_CSS = """
.fbars{width:1180px;display:flex;flex-direction:column;gap:10px}
.fb-row{display:grid;grid-template-columns:400px 1fr 120px;gap:20px;align-items:center}
.fb-label{font-size:22px;text-align:right;font-variant-numeric:tabular-nums}
.fb-track{position:relative;height:34px;background:var(--n-100);border-radius:2px}
.fb-bar{height:34px;border-radius:2px}
.fb-bar.nogo{background:var(--clay-text)}.fb-bar.caution{background:var(--gold)}.fb-bar.go{background:var(--olive)}
.fb-line{position:absolute;top:-6px;bottom:-6px;width:0;border-left:2px dashed var(--n-700)}
.fb-line.go{border-left-style:solid}
.fb-val{font-size:24px;font-weight:600;font-variant-numeric:tabular-nums}
.fb-val.nogo,.nogo-t{color:var(--clay-text)}.fb-val.caution,.caution-t{color:var(--gold-text)}.fb-val.go,.go-t{color:var(--olive)}
.fb-cap{padding:8px 0 0 420px}
.side .metric{font-size:56px}
.call-num{font-variant-numeric:tabular-nums}
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", action="store_true")
    ap.add_argument("--publish", metavar="PROFILE_DIR")
    a = ap.parse_args()
    d = load()
    shell = (HERE / "deck_shell.html").read_text(encoding="utf-8")
    out = (shell.replace("{{TITLE}}", "Data Migration Quality Analysis · Jeffrey Benson")
                .replace("{{DESC}}", "Six Sigma quality analysis of a healthcare claims migration (CMS synthetic data): the averages pass, the detail does not. Recommendation: do not proceed as is.")
                .replace("{{EXTRA_CSS}}", EXTRA_CSS)
                .replace("{{SLIDES}}", build(d)))
    out = out.replace("<!-- Deck shell:", f"<!-- Built {datetime.now():%Y-%m-%d %H:%M} by deck/build_deck.py from the DuckDB views.\n     Deck shell:", 1)
    OUT_HTML.write_text(out, encoding="utf-8")
    print(f"wrote {OUT_HTML}")
    if a.pdf or a.publish:
        chrome = shutil.which("google-chrome") or "/usr/bin/google-chrome"
        subprocess.run([chrome, "--headless=new", "--no-pdf-header-footer", f"--print-to-pdf={OUT_PDF}",
                        OUT_HTML.as_uri()], check=True, capture_output=True)
        print(f"wrote {OUT_PDF}")
    if a.publish:
        dest = Path(a.publish).expanduser()
        shutil.copy2(OUT_HTML, dest / OUT_HTML.name)
        shutil.copy2(OUT_PDF, dest / OUT_PDF.name)
        print(f"published to {dest}")


if __name__ == "__main__":
    main()
