# portfolio.py
import json
import re
from datetime import datetime, date, timedelta
from botocore.exceptions import ClientError


def sanitize_portfolio_name(name: str) -> str:
    """Keep names S3-safe and URL-safe-ish."""
    name = (name or "").strip()
    name = re.sub(r"[^a-zA-Z0-9_\- ]+", "", name)
    name = name.replace(" ", "_")
    return name[:64]


def s3_get_json(s3_client, bucket: str, key: str, default):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return default
        raise


def s3_put_json(s3_client, bucket: str, key: str, data):
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def list_portfolio_names(s3_client, bucket: str, user_id: str):
    """
    Returns portfolio folder names under:
    user_data/<user_id>/portfolio/<portfolio_name>/
    """
    base_prefix = f"user_data/{user_id}/portfolio/"
    resp = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=base_prefix,
        Delimiter="/"
    )
    prefixes = resp.get("CommonPrefixes", [])
    names = []
    for p in prefixes:
        parts = p["Prefix"].rstrip("/").split("/")
        if len(parts) >= 4:
            names.append(parts[-1])
    return sorted(names)


def create_portfolio(
    s3_client,
    bucket: str,
    user_id: str,
    portfolio_name_raw: str,
    start_date: str,
    end_date: str,
    starting_usd: float,
):
    """
    Writes:
      user_data/<user>/portfolio/<name>/meta.json
      user_data/<user>/portfolio/<name>/tx.json

    tx.json begins with an initial $USD "buy" if starting_usd > 0.
    """
    portfolio_name = sanitize_portfolio_name(portfolio_name_raw)
    if not portfolio_name:
        raise ValueError("Portfolio name required.")
    if not start_date:
        raise ValueError("Start date required.")
    if starting_usd is None or float(starting_usd) < 0:
        raise ValueError("Starting USD must be >= 0.")

    base_prefix = f"user_data/{user_id}/portfolio/{portfolio_name}/"
    tx_key = f"{base_prefix}tx.json"
    meta_key = f"{base_prefix}meta.json"

    txs = []
    starting_usd = float(starting_usd or 0)
    if starting_usd > 0:
        txs.append({
            "stock": "$USD",
            "date": start_date,
            "action": "buy",
            "quantity": starting_usd
        })

    meta = {
        "name": portfolio_name,
        "start_date": start_date,
        "end_date": end_date,
        "starting_usd": starting_usd,
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    s3_put_json(s3_client, bucket, tx_key, txs)
    s3_put_json(s3_client, bucket, meta_key, meta)

    return portfolio_name


def normalize_item_symbol(symbol: str) -> str:
    """
    Allows user to type usd, $usd, $, etc.
    Returns '$USD' or upper ticker.
    """
    s = (symbol or "").strip()
    if not s:
        return ""
    low = s.lower()
    if low in ("usd", "$usd", "$"):
        return "$USD"
    return s.upper()


def _parse_ymd(s: str):
    """
    Parses YYYY-MM-DD from:
    - YYYY-MM-DD
    - YYYY-MM-DDTHH:MM:SS...
    Returns datetime.date or None
    """
    if not s:
        return None
    s = str(s)
    if len(s) >= 10:
        s = s[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _tx_sort_key(tx: dict):
    """
    Sort chronologically, and for same-day ordering:
      0: USD deposit (buy $USD)
      1: stock sells
      2: stock buys
      3: USD withdrawal (sell $USD)
    This supports "deposit then buy same day" and is conservative for withdrawals.
    """
    d = str(tx.get("date", ""))[:10]
    stock = str(tx.get("stock", "")).strip().upper()
    action = str(tx.get("action", "")).strip().lower()

    if stock == "$USD" and action == "buy":
        p = 0
    elif stock != "$USD" and action == "sell":
        p = 1
    elif stock != "$USD" and action == "buy":
        p = 2
    elif stock == "$USD" and action == "sell":
        p = 3
    else:
        p = 9

    return (d, p, stock)


def compute_portfolio_value_simple(
    s3_client,
    bucket: str,
    tx_key: str,
    stock_data_prefix: str,
    portfolio_end_date: str = "",
):
    """
    Split-adjusted price history + split-aware holdings + mark-to-market.

    Price model:
      adjusted_price(date) = raw_price(date) / cumulativeFactor(date)
    This makes AAPL around ~$74 on 2020-01-02 if a later 4:1 split exists.

    Holdings:
      adj_quantity is stored in "end-of-series adjusted shares":
        buy q at date with factor f -> adj_qty += q * f
        sell q at date with factor f -> adj_qty -= q * f

    Cash:
      cash changes with adjusted transaction-day price:
        buy -> cash -= adjusted_open(date) * q
        sell -> cash += adjusted_open(date) * q
      USD deposit/withdrawal are literal dollars at their dates.

    Valuation:
      mark-to-market at portfolio_end_date (closest date) if provided;
      else latest available row.
    """
    txs = s3_get_json(s3_client, bucket, tx_key, default=[])
    txs_sorted = sorted(txs, key=_tx_sort_key)

    cash = 0.0
    holdings = {}  # ticker -> {"adj_quantity": float, "rows": [...], "ymd_index": {ymd: idx}, "ymd_list":[date|None]}
    end_dt = _parse_ymd(portfolio_end_date)

    def _load_stock(ticker: str):
        t = ticker.lower()
        if t in holdings and "rows" in holdings[t]:
            return

        key = f"{stock_data_prefix}{t}_data.json"
        rows = s3_get_json(s3_client, bucket, key, default=[])
        if not rows:
            holdings[t] = {"adj_quantity": 0.0, "rows": [], "ymd_index": {}, "ymd_list": []}
            return

        rows = sorted(rows, key=lambda r: str(r.get("date", "")))

        ymd_index = {}
        ymd_list = []
        for i, r in enumerate(rows):
            ymd = str(r.get("date", ""))[:10]
            ymd_index[ymd] = i
            ymd_list.append(_parse_ymd(ymd))

        # cumulativeFactor = reverse cumulative product of splitFactor
        cum_factors = [0.0] * len(rows)
        cum = 1.0
        for i in range(len(rows) - 1, -1, -1):
            sf = rows[i].get("splitFactor", 1.0)
            try:
                sf = float(sf or 1.0)
            except Exception:
                sf = 1.0
            cum *= sf
            cum_factors[i] = cum

        for i, r in enumerate(rows):
            r["_cumulativeFactor"] = cum_factors[i]

        holdings[t] = {
            "adj_quantity": holdings.get(t, {}).get("adj_quantity", 0.0),
            "rows": rows,
            "ymd_index": ymd_index,
            "ymd_list": ymd_list,
        }

    def _row_for_tx_date_forward(ticker: str, tx_date_str: str, max_forward_days: int = 10):
        _load_stock(ticker)
        info = holdings.get(ticker.lower(), {})
        rows = info.get("rows", [])
        ymd_index = info.get("ymd_index", {})
        if not rows:
            return None

        d0 = _parse_ymd(tx_date_str)
        if d0 is None:
            return rows[-1]

        d = d0
        for _ in range(max_forward_days + 1):
            ymd = d.strftime("%Y-%m-%d")
            idx = ymd_index.get(ymd)
            if idx is not None:
                return rows[idx]
            d += timedelta(days=1)

        return None

    def _row_for_closest_date(ticker: str, target_date: date):
        _load_stock(ticker)
        info = holdings.get(ticker.lower(), {})
        rows = info.get("rows", [])
        ymd_list = info.get("ymd_list", [])
        if not rows:
            return None

        best_i = None
        best_diff = None
        for i, d in enumerate(ymd_list):
            if d is None:
                continue
            diff = abs((d - target_date).days)
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_i = i

        if best_i is None:
            return rows[-1]
        return rows[best_i]

    def _adjusted_price(row: dict, field: str) -> float:
        try:
            raw = float(row.get(field, 0) or 0)
        except Exception:
            return 0.0
        try:
            factor = float(row.get("_cumulativeFactor", 1.0) or 1.0)
        except Exception:
            factor = 1.0
        if factor == 0:
            return raw
        return raw / factor

    def _tx_price_open(row: dict) -> float:
        return _adjusted_price(row, "open")

    def _val_price_close(row: dict) -> float:
        c = _adjusted_price(row, "close")
        if c != 0:
            return c
        return _adjusted_price(row, "open")

    # Apply transactions (no validation here; this is just valuation)
    for tx in txs_sorted:
        stock = str(tx.get("stock", "")).strip()
        action = str(tx.get("action", "")).lower().strip()
        qty = float(tx.get("quantity", 0) or 0)
        tx_date = str(tx.get("date", "")).strip()

        if qty <= 0 or action not in ("buy", "sell") or not stock:
            continue

        if stock.upper() == "$USD":
            cash += qty if action == "buy" else -qty
            continue

        ticker = stock.lower()
        row = _row_for_tx_date_forward(ticker, tx_date, max_forward_days=10)
        if row is None:
            continue

        price = _tx_price_open(row)
        factor = float(row.get("_cumulativeFactor", 1.0) or 1.0)

        adj_qty = float(holdings.get(ticker, {}).get("adj_quantity", 0.0) or 0.0)
        shares_at_tx_date = adj_qty / factor if factor != 0 else 0.0

        if action == "buy":
            cash -= price * qty
            adj_qty += qty * factor
            holdings[ticker]["adj_quantity"] = adj_qty

        elif action == "sell":
            if qty > shares_at_tx_date + 1e-9:
                continue
            cash += price * qty
            adj_qty -= qty * factor
            holdings[ticker]["adj_quantity"] = adj_qty

    # Mark-to-market at end date (or latest)
    asset_value = 0.0
    for ticker, info in holdings.items():
        rows = info.get("rows", [])
        if not rows:
            continue

        adj_qty = float(info.get("adj_quantity", 0.0) or 0.0)
        if abs(adj_qty) < 1e-12:
            continue

        if end_dt is None:
            end_row = rows[-1]
        else:
            end_row = _row_for_closest_date(ticker, end_dt) or rows[-1]

        end_factor = float(end_row.get("_cumulativeFactor", 1.0) or 1.0)
        shares_at_end = adj_qty / end_factor if end_factor != 0 else 0.0
        end_price = _val_price_close(end_row)

        asset_value += shares_at_end * end_price

    return {
        "cash_value": cash,
        "asset_value": asset_value,
        "total_value": cash + asset_value,
        "txs": txs_sorted
    }


def _validate_cash_constraints_or_raise(
    s3_client,
    bucket: str,
    txs: list,
    stock_data_prefix: str,
):
    """
    Simulate transactions in time order and enforce:
      - Cannot buy stock if cash < cost at that date
      - Cannot withdraw USD if cash < amount at that date
    If violation occurs, raise ValueError and include a helpful message.
    """
    txs_sorted = sorted(txs, key=_tx_sort_key)
    cash = 0.0

    # We need split-adjusted pricing per ticker/date; cache per ticker
    cache = {}  # ticker -> dict with rows, ymd_index, and cumulativeFactors

    def _load_stock_cached(ticker: str):
        t = ticker.lower()
        if t in cache:
            return cache[t]

        key = f"{stock_data_prefix}{t}_data.json"
        rows = s3_get_json(s3_client, bucket, key, default=[])
        rows = sorted(rows, key=lambda r: str(r.get("date", "")))

        ymd_index = {}
        ymd_list = []
        for i, r in enumerate(rows):
            ymd = str(r.get("date", ""))[:10]
            ymd_index[ymd] = i
            ymd_list.append(_parse_ymd(ymd))

        # cumulativeFactor reverse cumulative product
        cum_factors = [0.0] * len(rows)
        cum = 1.0
        for i in range(len(rows) - 1, -1, -1):
            sf = rows[i].get("splitFactor", 1.0)
            try:
                sf = float(sf or 1.0)
            except Exception:
                sf = 1.0
            cum *= sf
            cum_factors[i] = cum

        for i, r in enumerate(rows):
            r["_cumulativeFactor"] = cum_factors[i]

        cache[t] = {"rows": rows, "ymd_index": ymd_index}
        return cache[t]

    def _row_for_tx_date_forward_cached(ticker: str, tx_date_str: str, max_forward_days: int = 10):
        info = _load_stock_cached(ticker)
        rows = info["rows"]
        ymd_index = info["ymd_index"]
        if not rows:
            return None

        d0 = _parse_ymd(tx_date_str)
        if d0 is None:
            return rows[-1]

        d = d0
        for _ in range(max_forward_days + 1):
            ymd = d.strftime("%Y-%m-%d")
            idx = ymd_index.get(ymd)
            if idx is not None:
                return rows[idx]
            d += timedelta(days=1)

        return None

    def _adjusted_price(row: dict, field: str) -> float:
        try:
            raw = float(row.get(field, 0) or 0)
        except Exception:
            return 0.0
        try:
            factor = float(row.get("_cumulativeFactor", 1.0) or 1.0)
        except Exception:
            factor = 1.0
        if factor == 0:
            return raw
        return raw / factor

    for tx in txs_sorted:
        stock = str(tx.get("stock", "")).strip()
        action = str(tx.get("action", "")).lower().strip()
        qty = float(tx.get("quantity", 0) or 0)
        tx_date = str(tx.get("date", "")).strip()

        if qty <= 0 or action not in ("buy", "sell") or not stock:
            continue

        if stock.upper() == "$USD":
            if action == "buy":
                cash += qty
            else:
                # withdrawal must have cash at that date
                if cash + 1e-9 < qty:
                    raise ValueError(
                        f"Insufficient USD on {tx_date}: tried to withdraw ${qty:.2f} but only had ${cash:.2f}."
                    )
                cash -= qty
            continue

        # stock transaction: enforce cash on buys only
        row = _row_for_tx_date_forward_cached(stock.lower(), tx_date, max_forward_days=10)
        if row is None:
            raise ValueError(f"No price data found for {stock} around {tx_date}.")

        px = _adjusted_price(row, "open")  # match portfolio transaction pricing
        cost = px * qty

        if action == "buy":
            if cash + 1e-9 < cost:
                raise ValueError(
                    f"Insufficient USD on {tx_date} to buy {qty:g} {stock}. "
                    f"Cost ≈ ${cost:.2f} (px≈${px:.2f}) but cash was ${cash:.2f}."
                )
            cash -= cost
        else:
            # sell increases cash
            cash += cost


def filter_transactions(txs, action="", item="", start_date="", end_date=""):
    f_action = (action or "").lower().strip()
    f_item = (item or "").strip()
    f_start = (start_date or "").strip()
    f_end = (end_date or "").strip()

    out = []
    for tx in txs:
        if f_action and str(tx.get("action", "")).lower() != f_action:
            continue
        if f_item and str(tx.get("stock", "")).lower() != f_item.lower():
            continue
        d = str(tx.get("date", ""))
        if f_start and d < f_start:
            continue
        if f_end and d > f_end:
            continue
        out.append(tx)

    return out


def add_portfolio_transaction(
    s3_client,
    bucket: str,
    tx_key: str,
    stock_data_prefix: str,
    stock: str,
    date: str,
    action: str,
    quantity: float,
):
    """
    Adds a transaction ONLY if it passes dated-cash validation.
    """
    stock_norm = normalize_item_symbol(stock)
    action = (action or "").lower().strip()
    quantity = float(quantity or 0)
    date = (date or "").strip()

    if not stock_norm or not date or action not in ("buy", "sell") or quantity <= 0:
        raise ValueError("Invalid transaction input.")

    # Load existing txs, append candidate, validate, then write if OK
    txs = s3_get_json(s3_client, bucket, tx_key, default=[])
    candidate = {
        "stock": stock_norm,
        "date": date,
        "action": action,
        "quantity": quantity
    }
    proposed = txs + [candidate]

    _validate_cash_constraints_or_raise(
        s3_client=s3_client,
        bucket=bucket,
        txs=proposed,
        stock_data_prefix=stock_data_prefix,
    )

    # If validation passes, persist
    txs.append(candidate)
    s3_put_json(s3_client, bucket, tx_key, txs)


def delete_portfolio_prefix(s3_client, bucket: str, prefix: str):
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    to_delete = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
    if to_delete:
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})

# -----------------------------
# Rule application helpers (Portfolio)
# -----------------------------

def list_rule_files(s3_client, bucket: str, user_id: str, rule_type: str):
    """
    Lists .json rule files under:
      user_data/<user_id>/<dca|fqr>/
    """
    rule_type = (rule_type or "").lower().strip()
    if rule_type not in ("dca", "fqr"):
        return []

    prefix = f"user_data/{user_id}/{rule_type}/"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    for obj in resp.get("Contents", []):
        key = obj.get("Key", "")
        if key.endswith(".json"):
            files.append(key.split("/")[-1])
    return sorted(files)


def load_rule_json(s3_client, bucket: str, user_id: str, rule_type: str, rule_file: str):
    """
    Loads rule JSON from S3. Supports:
      - [ { ... } ] (your current pattern)
      - { ... }
    """
    rule_type = (rule_type or "").lower().strip()
    if rule_type not in ("dca", "fqr"):
        raise ValueError("Invalid rule type.")

    rule_file = (rule_file or "").strip()
    if not rule_file.endswith(".json"):
        raise ValueError("Invalid rule file.")

    key = f"user_data/{user_id}/{rule_type}/{rule_file}"
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    data = json.loads(obj["Body"].read().decode("utf-8"))

    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict):
        return data
    raise ValueError("Rule file is empty or invalid.")


def _as_ymd(s: str) -> str:
    s = (s or "").strip()
    return s[:10] if len(s) >= 10 else s


def _iter_rule_dates(freq: str, start_ymd: str, end_ymd: str):
    """
    Generate schedule dates between start/end inclusive.
    - monthly: first day of each month
    - weekly: every 7 days from start
    Returns list of YYYY-MM-DD strings.
    """
    freq = (freq or "weekly").lower().strip()
    sdt = _parse_ymd(start_ymd)
    edt = _parse_ymd(end_ymd)
    if sdt is None or edt is None:
        return []

    dates = []
    if freq == "monthly":
        cur = date(sdt.year, sdt.month, 1)
        while cur <= edt:
            if cur >= sdt:
                dates.append(cur.strftime("%Y-%m-%d"))
            # advance month
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)
    else:
        cur = sdt
        while cur <= edt:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur = cur + timedelta(days=7)

    return dates


def _get_adjusted_open_for_date(s3_client, bucket: str, stock_data_prefix: str, ticker: str, ymd: str):
    """
    Returns split-adjusted open price for a given date using:
      adjusted_open = raw_open / cumulativeFactor(date)
    If exact date missing, search forward up to 10 days (same as portfolio tx logic).
    Returns (price, used_ymd) or (None, None)
    """
    ticker = (ticker or "").lower().strip()
    if not ticker:
        return None, None

    key = f"{stock_data_prefix}{ticker}_data.json"
    rows = s3_get_json(s3_client, bucket, key, default=[])
    if not rows:
        return None, None

    rows = sorted(rows, key=lambda r: str(r.get("date", "")))

    # index by ymd
    ymd_index = {str(r.get("date", ""))[:10]: i for i, r in enumerate(rows)}

    # compute cumulativeFactor reverse cumulative product of splitFactor
    cum_factors = [0.0] * len(rows)
    cum = 1.0
    for i in range(len(rows) - 1, -1, -1):
        sf = rows[i].get("splitFactor", 1.0)
        try:
            sf = float(sf or 1.0)
        except Exception:
            sf = 1.0
        cum *= sf
        cum_factors[i] = cum

    for i, r in enumerate(rows):
        r["_cumulativeFactor"] = cum_factors[i]

    d0 = _parse_ymd(ymd)
    if d0 is None:
        r = rows[-1]
        raw = float(r.get("open", 0) or 0)
        f = float(r.get("_cumulativeFactor", 1.0) or 1.0)
        return (raw / f if f else raw), str(r.get("date", ""))[:10]

    d = d0
    for _ in range(11):  # 0..10 forward
        try_ymd = d.strftime("%Y-%m-%d")
        idx = ymd_index.get(try_ymd)
        if idx is not None:
            r = rows[idx]
            raw = float(r.get("open", 0) or 0)
            f = float(r.get("_cumulativeFactor", 1.0) or 1.0)
            return (raw / f if f else raw), try_ymd
        d += timedelta(days=1)

    return None, None


def generate_rule_transactions(s3_client, bucket: str, stock_data_prefix: str, rule_type: str, rule: dict):
    """
    Create BUY transactions from DCA/FQR rule definition.
      - DCA: fixed_dollar_amount / adjusted_open
      - FQR: fixed_quantity
    Output txs match portfolio tx schema: {stock,date,action,quantity}
    """
    rule_type = (rule_type or "").lower().strip()
    if rule_type not in ("dca", "fqr"):
        raise ValueError("Invalid rule type.")

    stock = (rule.get("stock") or "").lower().strip()
    if not stock:
        raise ValueError("Rule missing stock.")

    freq = (rule.get("frequency") or "weekly").lower().strip()
    start_ymd = _as_ymd(rule.get("start_date", ""))
    end_ymd = _as_ymd(rule.get("end_date", ""))

    if not start_ymd or not end_ymd:
        raise ValueError("Rule missing start_date/end_date.")

    fixed_dollars = float(rule.get("fixed_dollar_amount", 0) or 0)
    fixed_qty = float(rule.get("fixed_quantity", 0) or 0)

    dates = _iter_rule_dates(freq, start_ymd, end_ymd)
    out = []

    for ymd in dates:
        px, used_ymd = _get_adjusted_open_for_date(s3_client, bucket, stock_data_prefix, stock, ymd)
        if px is None or px <= 0:
            continue

        if rule_type == "dca":
            if fixed_dollars <= 0:
                continue
            qty = fixed_dollars / px
        else:
            if fixed_qty <= 0:
                continue
            qty = fixed_qty

        out.append({
            "stock": stock.upper(),
            "date": used_ymd,
            "action": "buy",
            "quantity": float(qty),
        })

    out.sort(key=lambda t: (t.get("date", ""), t.get("stock", "")))
    return out


def apply_rule_to_portfolio_partial(
    s3_client,
    bucket: str,
    tx_key: str,
    stock_data_prefix: str,
    rule_txs: list,
):
    """
    Apply rule transactions sequentially until funds run out.
    Uses your existing dated-cash validator (_validate_cash_constraints_or_raise).
    Persists only the applied subset.
    Returns (applied, skipped, msg)
    """
    existing = s3_get_json(s3_client, bucket, tx_key, default=[])
    working = list(existing)

    applied = []
    skipped = []

    for i, tx in enumerate(rule_txs):
        candidate = {
            "stock": normalize_item_symbol(tx.get("stock", "")),
            "date": _as_ymd(tx.get("date", "")),
            "action": (tx.get("action", "") or "").lower().strip(),
            "quantity": float(tx.get("quantity", 0) or 0),
        }

        # basic validation
        if (
            not candidate["stock"]
            or not candidate["date"]
            or candidate["action"] not in ("buy", "sell")
            or candidate["quantity"] <= 0
        ):
            skipped.append({**tx, "reason": "invalid tx"})
            continue

        proposed = working + [candidate]

        try:
            _validate_cash_constraints_or_raise(
                s3_client=s3_client,
                bucket=bucket,
                txs=proposed,
                stock_data_prefix=stock_data_prefix,
            )
        except ValueError as e:
            # funds exhausted -> skip this and the rest
            skipped.append({**tx, "reason": str(e)})
            for later in rule_txs[i + 1:]:
                skipped.append({**later, "reason": "skipped after funds exhausted"})
            break

        working.append(candidate)
        applied.append(candidate)

    if applied:
        s3_put_json(s3_client, bucket, tx_key, working)

    msg = f"Rule generated {len(rule_txs)} txs. Applied {len(applied)}. Skipped {len(skipped)}."
    return applied, skipped, msg
