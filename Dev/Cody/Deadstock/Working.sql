SELECT
  inv_mast.item_id
 ,SUM(inv_loc.qty_on_hand) AS QTY
 ,MAX(invoice_line.date_created) AS [Last Invoiced]
 ,inv_mast.class_id2
 ,MIN(po_line.received_date) AS [First Received]
FROM dbo.inv_mast
INNER JOIN dbo.inv_loc
  ON inv_loc.inv_mast_uid = inv_mast.inv_mast_uid
LEFT OUTER JOIN dbo.po_line
  ON inv_mast.inv_mast_uid = po_line.inv_mast_uid
LEFT OUTER JOIN dbo.invoice_line
  ON inv_mast.inv_mast_uid = invoice_line.inv_mast_uid
GROUP BY inv_mast.item_id
        ,inv_mast.class_id2



SELECT
  inv_mast.item_id
 ,SUM(inv_loc.qty_on_hand) AS QTY
 ,MAX(invoice_line.date_created) AS [Last Invoiced]
 ,inv_mast.class_id2
 ,Items_Last_Receipt_Date.last_receipt_date AS [Last Receipt]
FROM dbo.inv_mast
INNER JOIN dbo.inv_loc
  ON inv_loc.inv_mast_uid = inv_mast.inv_mast_uid
LEFT OUTER JOIN dbo.invoice_line
  ON inv_mast.inv_mast_uid = invoice_line.inv_mast_uid
LEFT OUTER JOIN dbo.Items_Last_Receipt_Date
  ON inv_mast.item_id = Items_Last_Receipt_Date.item_id
GROUP BY inv_mast.item_id
        ,inv_mast.class_id2
        ,Items_Last_Receipt_Date.last_receipt_date
