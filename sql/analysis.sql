# Data Understanding

## subscription_snapshot_fin
SELECT * from `test_analyst.subscription_snapshot_fin` limit 10

SELECT count(*) from `test_analyst.subscription_snapshot_fin`

SELECT
  column_name,
  data_type,
  is_nullable
FROM `test_analyst.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'subscription_snapshot_fin'

## servco_fin
SELECT * from `test_analyst.servco_fin` limit 10

SELECT count(*) from `test_analyst.servco_fin`

SELECT
  column_name,
  data_type,
  is_nullable
FROM `test_analyst.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'servco_fin'

## region_master_fin
SELECT * from `test_analyst.region_master_fin` limit 10

SELECT count(*) from `test_analyst.region_master_fin`

SELECT
  column_name,
  data_type,
  is_nullable
FROM `test_analyst.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'region_master_fin'

## media_package_fin
SELECT * from `test_analyst.media_package_fin` limit 10

SELECT count(*) from `test_analyst.media_package_fin`

SELECT
  column_name,
  data_type,
  is_nullable
FROM `test_analyst.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'media_package_fin'

## homepass_fin
SELECT * from `test_analyst.homepass_fin` limit 10

SELECT count(*) from `test_analyst.homepass_fin`

SELECT
  column_name,
  data_type,
  is_nullable
FROM  `test_analyst.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'homepass_fin'


# Exploratory Data Analysis

# Section 1 - Infrastructure & Utilization

## 1.1 Calculate monthly penetration rate

-- 1.1.1 penetration by region
SELECT
  h.region,
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT s.contract_account), COUNT(DISTINCT h.homeid)) * 100, 2) AS penetration_rate
FROM `test_analyst.homepass_fin` h
LEFT JOIN `test_analyst.subscription_snapshot_fin` s 
  ON h.homeid = s.homeid 
  AND s.active_flag = 1
GROUP BY h.region, month
ORDER BY h.region, month

-- 1.1.2 penetration by technology
SELECT
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  h.technology,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
WHERE s.active_flag = 1
GROUP BY month,h.technology
ORDER BY month,h.technology


-- 1.1.3 penetration by areas
SELECT
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  CASE
    WHEN h.exclusive_flag is TRUE THEN 'Exclusive'
    ELSE 'Open Access'
  END AS area_type,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
WHERE s.active_flag = 1
GROUP BY month,area_type
ORDER BY month,area_type


## 1.2 Identify High capex but low utilization areas and Underperforming fibernodes

-- 1.2.1 High capex but low utilization areas
SELECT
  h.fibernode,
  SUM(h.capex_cost) AS total_capex,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate
FROM `test_analyst.homepass_fin` h
LEFT JOIN `test_analyst.subscription_snapshot_fin` s
  ON h.homeid = s.homeid
  AND s.active_flag = 1
GROUP BY h.fibernode
ORDER BY total_capex DESC

-- 1.2.2 Underperforming fibernodes
SELECT
  h.fibernode,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate
FROM `test_analyst.homepass_fin` h
LEFT JOIN `test_analyst.subscription_snapshot_fin` s
  ON h.homeid = s.homeid
  AND s.active_flag = 1
GROUP BY h.fibernode
ORDER BY penetration_rate ASC

## 1.3 Compare monetization trend

-- 1.3.1 between Exclusive vs open access areas
SELECT
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  SUM(
    CASE 
      WHEN h.exclusive_flag IS TRUE 
      THEN m.package_price ELSE 0 
    END) AS revenue_exclusive,
  SUM(
    CASE 
      WHEN h.exclusive_flag IS FALSE 
      THEN m.package_price ELSE 0 
    END) AS revenue_open_access
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
JOIN `test_analyst.media_package_fin` m
  ON s.contract_account = m.contract_account
WHERE s.active_flag = 1
GROUP BY month
ORDER BY month


-- 1.3.2 between FTTH vs HFC
SELECT
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  SUM(
    CASE 
      WHEN h.technology = "FTTH" 
      THEN m.package_price ELSE 0 
    END) AS revenue_FTTH,
  SUM(
    CASE 
      WHEN h.technology = "HFC" 
      THEN m.package_price ELSE 0 
    END) AS revenue_HFC,
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
JOIN `test_analyst.media_package_fin` m
  ON s.contract_account = m.contract_account
WHERE s.active_flag = 1
GROUP BY month
ORDER BY month


# Section 2 - Servco Performance & Competition

## 2.1 Analyze Servco performance

-- 2.1.1 across Active CA growth
WITH monthly_stats AS (
  SELECT
    sv.servco_name,
    FORMAT_DATE('%m', s.snapshot_date) AS month,
    COUNT(DISTINCT s.contract_account) AS current_active_ca,
    LAG(COUNT(DISTINCT s.contract_account)) OVER(
      PARTITION BY sv.servco_name 
      ORDER BY MIN(s.snapshot_date)
    ) AS prev_month_ca
  FROM `test_analyst.subscription_snapshot_fin` s
  JOIN `test_analyst.servco_fin` sv ON s.servco_id = sv.servco_id
  WHERE s.active_flag = 1
  GROUP BY sv.servco_name, month
)
SELECT 
  *,
  ROUND(SAFE_DIVIDE((current_active_ca - prev_month_ca), prev_month_ca) * 100, 2) AS growth_percentage
FROM monthly_stats
ORDER BY servco_name, month

-- 2.1.2 across Lease revenue contribution
SELECT
  s.servco_id,
  sv.servco_name,
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  sv.lease_fee_per_active,
  COUNT(DISTINCT s.contract_account) * sv.lease_fee_per_active AS lease_revenue
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.servco_fin` sv
  ON s.servco_id = sv.servco_id
WHERE s.active_flag = 1
GROUP BY s.servco_id, sv.servco_name, month, sv.lease_fee_per_active
ORDER BY s.servco_id, month ASC

-- 2.1.3 across Penetration rate
SELECT
  s.servco_id,
  sv.servco_name,
  FORMAT_DATE('%m', s.snapshot_date) AS month,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
JOIN `test_analyst.servco_fin` sv
  ON s.servco_id = sv.servco_id
WHERE s.active_flag = 1
GROUP BY s.servco_id, sv.servco_name, month
ORDER BY s.servco_id, month ASC

## 2.2 Evaluate Servco 101 performance and penetration against minimum guarantee
SELECT
  sv.servco_name,
  sv.minimum_guarantee, 
  COUNT(DISTINCT s.contract_account) AS active_ca,
    COUNT(DISTINCT s.contract_account) - sv.minimum_guarantee AS gap_to_target,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate,
  sv.lease_fee_per_active,
  COUNT(DISTINCT s.contract_account) * sv.lease_fee_per_active AS lease_revenue,  
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.servco_fin` sv
  ON s.servco_id = sv.servco_id
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
WHERE s.active_flag = 1
AND s.servco_id = 101
GROUP BY sv.servco_name, sv.minimum_guarantee, sv.lease_fee_per_active

## 2.3 Compare performance

-- 2.3.1 In exclusive window vs post-exclusive
SELECT
  CASE
    WHEN DATE(s.activation_date) BETWEEN h.exclusive_start_date  AND h.exclusive_end_date
    THEN 'Exclusive Window'
    ELSE 'Post Exclusive'
  END AS period_type,
  COUNT(DISTINCT s.contract_account) AS active_ca,
  COUNT(DISTINCT h.homeid) AS total_homepass,
  ROUND(COUNT(DISTINCT s.contract_account) / COUNT(DISTINCT h.homeid)*100, 2) AS penetration_rate,
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
WHERE s.active_flag = 1
GROUP BY period_type

-- 2.3.2 In single-ISP vs multi-ISP areas
SELECT
  CASE
    WHEN servco_count = 1 THEN 'Single ISP'
    ELSE 'Multi ISP'
  END AS market_type,
  COUNT(*) AS fibernode_count
FROM (
  SELECT
    h.fibernode,
    COUNT(DISTINCT s.servco_id) AS servco_count
  FROM `test_analyst.homepass_fin` h
  LEFT JOIN `test_analyst.subscription_snapshot_fin` s
    ON h.homeid = s.homeid
    AND s.active_flag = 1
  GROUP BY h.fibernode
)
GROUP BY market_type

# Section 3 - Media Monetization & Profitability

## 3.1 Calculate media attach rate per Servco and per region
SELECT
  h.region,
  COUNT(DISTINCT s.contract_account) AS total_subs,
  COUNT(DISTINCT m.contract_account) AS media_subs,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT m.contract_account), COUNT(DISTINCT s.contract_account)) * 100, 2) AS attach_rate
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h ON s.homeid = h.homeid
LEFT JOIN `test_analyst.media_package_fin` m ON s.contract_account = m.contract_account
WHERE s.active_flag = 1
GROUP BY h.region
ORDER BY attach_rate DESC;

## 3.2 Analyze contribution

-- 3.2.1 Lease revenue vs Media revenue
SELECT
  SUM(sv.lease_fee_per_active) AS lease_revenue,
  SUM(m.package_price) AS media_revenue
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.servco_fin` sv
  ON s.servco_id = sv.servco_id
LEFT JOIN `test_analyst.media_package_fin` m
  ON s.contract_account = m.contract_account
WHERE s.active_flag = 1

-- 3.2.2 Media margin by product
SELECT
  add_on_product,
  AVG(ao_rrp_price - ao_wholesale_price) AS avg_margin
FROM `test_analyst.media_package_fin`
GROUP BY add_on_product
ORDER BY avg_margin DESC

## 3.3 Identify

-- 3.3.1 Most profitable media product
SELECT
  add_on_product,
  SUM(ao_rrp_price - ao_wholesale_price) AS total_profit
FROM `test_analyst.media_package_fin`
GROUP BY add_on_product
ORDER BY total_profit DESC

-- 3.3.2 Servco with strongest upsell capability
SELECT
  sv.servco_name,
  COUNT(DISTINCT m.contract_account) AS media_users,
  COUNT(DISTINCT s.contract_account) AS total_subscribers,
  ROUND(COUNT(DISTINCT m.contract_account) / COUNT(DISTINCT s.contract_account)*100, 2) AS upsell_rate
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.servco_fin` sv
  ON s.servco_id = sv.servco_id
LEFT JOIN `test_analyst.media_package_fin` m
  ON s.contract_account = m.contract_account
WHERE s.active_flag = 1
GROUP BY sv.servco_name
ORDER BY upsell_rate DESC

-- 3.3.3 Regions with highest monetization potential
SELECT
  h.region,
  SUM(m.package_price) AS total_media_revenue,
  COUNT(DISTINCT s.contract_account) AS subscribers,
  ROUND(SUM(m.package_price) / COUNT(DISTINCT s.contract_account), 2) AS media_revenue_per_user
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.homepass_fin` h
  ON s.homeid = h.homeid
LEFT JOIN `test_analyst.media_package_fin` m
  ON s.contract_account = m.contract_account
WHERE s.active_flag = 1
GROUP BY h.region
ORDER BY media_revenue_per_user DESC

## 3.4 Is media bundling meaningfully improving ARPU?
SELECT
  CASE WHEN m.contract_account IS NULL THEN 'Internet Only' ELSE 'Internet + Media' END AS package_type,
  COUNT(DISTINCT s.contract_account) AS user_count,
  ROUND(AVG(sv.lease_fee_per_active + IFNULL(m.package_price, 0)), 2) AS avg_arpu
FROM `test_analyst.subscription_snapshot_fin` s
JOIN `test_analyst.servco_fin` sv ON s.servco_id = sv.servco_id
LEFT JOIN `test_analyst.media_package_fin` m ON s.contract_account = m.contract_account
WHERE s.active_flag = 1
GROUP BY package_type