/* === Anchors for day-granularity windows (avoid TZ/off-by-one flaps) === */
DECLARE @today_utc date = CAST(SYSUTCDATETIME() AS date);
DECLARE @d45  date = DATEADD(DAY, -45, @today_utc);
DECLARE @d90  date = DATEADD(DAY, -90, @today_utc);

/* === Pre-aggregations to kill fan-out === */
WITH qty_by_item AS (
    SELECT im.item_id,
           COALESCE(SUM(iloc.qty_on_hand), 0) AS qty
    FROM dbo.inv_mast im
    LEFT JOIN dbo.inv_loc iloc
           ON iloc.inv_mast_uid = im.inv_mast_uid
    GROUP BY im.item_id
),
last_invoice_by_item AS (
    SELECT im.item_id,
           MAX(il.date_created) AS last_invoiced
    FROM dbo.inv_mast im
    LEFT JOIN dbo.invoice_line il
           ON il.inv_mast_uid = im.inv_mast_uid
    GROUP BY im.item_id
),
first_receipt_by_item AS (
    SELECT im.item_id,
           /* Ignore NULLs in the MIN to avoid NULL swallowing real dates */
           MIN(CASE WHEN pl.received_date IS NOT NULL THEN pl.received_date END) AS first_received
    FROM dbo.inv_mast im
    LEFT JOIN dbo.po_line pl
           ON pl.inv_mast_uid = im.inv_mast_uid
    GROUP BY im.item_id
)

/* === Final classification with explicit precedence === */
SELECT
    im.item_id,
    q.qty,
    li.last_invoiced,
    im.class_id2,
    fr.first_received,
    CASE
        /* 1) Seasonal override (human-curated wins) */
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%'
            THEN im.class_id2

        /* 2) No stock on hand */
        WHEN q.qty = 0
            THEN NULL

        /* 3) Was DEAD, but sold within last 90 days -> UNDEAD */
        WHEN im.class_id2 IS NOT NULL
         AND LOWER(im.class_id2) LIKE '%dead%'
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) >= @d90
            THEN 'UNDEAD'

        /* 4) DEAD (independent of current class), stocked & stale */
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        )
            THEN 'DEAD'

        /* 5) INACTIVE: 46–90 day quiet window, only if currently unclassified */
        WHEN q.qty > 0
         AND im.class_id2 IS NULL
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) <  @d45  -- older than 45 days
         AND CAST(li.last_invoiced AS date) >= @d90  -- within 90 days
            THEN 'INACTIVE'

        /* 6) Otherwise active/normal */
        ELSE NULL
    END AS status
FROM dbo.inv_mast im
LEFT JOIN qty_by_item          q  ON q.item_id  = im.item_id
LEFT JOIN last_invoice_by_item li ON li.item_id = im.item_id
LEFT JOIN first_receipt_by_item fr ON fr.item_id = im.item_id

/* ---- Return only rows that actually need updating ----*/
WHERE NOT (
    (im.class_id2 IS NULL AND
     CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN im.class_id2
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'UNDEAD'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'DEAD'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN 'INACTIVE'
        ELSE NULL
     END IS NULL)
 OR
    (im.class_id2 IS NOT NULL AND
     im.class_id2 = CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN im.class_id2
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'UNDEAD'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'DEAD'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN 'INACTIVE'
        ELSE NULL
     END)
)