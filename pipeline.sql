

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cc92d73d-a942-4f57-bc8a-14ffc8f1c177"),
    All_concept_set=Input(rid="ri.foundry.main.dataset.80e12731-f476-4faa-aee4-fb64f7a0bdff"),
    censored_cohort_60=Input(rid="ri.foundry.main.dataset.c29948a5-d3f1-473b-be51-9a1b70e73254"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b"),
    person1=Input(rid="ri.foundry.main.dataset.af5e5e91-6eeb-4b14-86df-18d84a5aa010")
)
----this is first verison for table 1,include 15 columns--------------------
-----age into decade, time zero period------------
-----------------------------------------------------------temporary table for medical history--------------------------------------
WITH ConditionCTE AS (
    SELECT
        a.person_id,
        MAX(CASE WHEN c.group_number = 3 THEN 1 ELSE 0 END) AS past_AKI,
        MAX(CASE WHEN c.group_number = 1 THEN 1 ELSE 0 END) AS hypertension,   ----1320106
        MAX(CASE WHEN c.group_number = 2 THEN 1 ELSE 0 END) AS diabetes_mellitus,---278113
        MAX(CASE WHEN c.group_number = 4 THEN 1 ELSE 0 END) AS heart_failure,
        MAX(CASE WHEN c.group_number = 5 THEN 1 ELSE 0 END) AS cardiovascular_disease,
        MAX(CASE WHEN c.group_number = 6 THEN 1 ELSE 0 END) AS obesity
    FROM censored_cohort_60 a
    LEFT JOIN condition_occurrence s ON a.person_id = s.person_id
    LEFT JOIN All_concept_set c ON s.condition_concept_id = c.concept_id
    WHERE s.condition_start_date < time_zero
      AND DATEDIFF(time_zero, s.condition_start_date) < 365
    GROUP BY a.person_id
)

-------------------------------正式表------------------------------------------------------------------------------------------
SELECT DISTINCT a.group_id, a.person_id, a.time_zero, has_AKI, AKI_interval_2,

------------------------gender,不明确的直接扔掉-------------------------------
CASE 
  WHEN b.gender_concept_id = 8532 THEN 'F'
  WHEN b.gender_concept_id = 8507 THEN 'M'
  ELSE NULL 
END AS gender,
-- -------------------准确的年龄列,大于90直接扔掉------------------------------
CAST((2023 - year_of_birth) AS decimal(10,2)) AS age_num,

-- 创建年龄分组列------------------------------------------------------------
CASE
  WHEN year_of_birth IS NOT NULL AND NOT is_age_90_or_older THEN
    CASE
      WHEN CAST(2023 - year_of_birth AS integer) < 30 THEN '<30'
      WHEN CAST(2023 - year_of_birth AS integer) BETWEEN 30 AND 49 THEN '30-49'
      WHEN CAST(2023 - year_of_birth AS integer) BETWEEN 50 AND 64 THEN '50-64'
      WHEN CAST(2023 - year_of_birth AS integer) >= 65 AND CAST(2023 - year_of_birth AS integer) <= 90 THEN '65-90'
      ELSE NULL
    END
  ELSE NULL
END AS age_category,
---race-------------
CASE 
  WHEN race_concept_id = 8527 THEN 'white'
  WHEN race_concept_id = 8516 THEN 'black'
  WHEN race_concept_id = 8515 THEN 'asian' 
  WHEN race_concept_id IS NULL OR race_concept_id = 0 THEN 'no info'
  ELSE 'other'
END AS race,
----ethnicity--------------------
CASE 
  WHEN ethnicity_concept_id = 38003564 THEN 'Not Hispanic or Latino'
  WHEN ethnicity_concept_id = 38003563  THEN 'Hispanic or Latino'
  ELSE 'Unknown'
END AS ethnicity,
--death_date-------------------------------------------------------------------------------
death_date,
--medical history in one year before T0-----------------------------------------------------
       COALESCE(past_AKI, 0) AS past_AKI,
       COALESCE(hypertension, 0) AS hypertension,
       COALESCE(diabetes_mellitus, 0) AS diabetes_mellitus,
       COALESCE(heart_failure, 0) AS heart_failure,
       COALESCE(cardiovascular_disease, 0) AS cardiovascular_disease,
       COALESCE(obesity, 0) AS obesity

FROM censored_cohort_60 a
INNER JOIN person1 b ON a.person_id = b.person_id
LEFT JOIN ConditionCTE c ON a.person_id = c.person_id
WHERE (year_of_birth IS not NULL ) AND ( b.gender_concept_id IN ('8532', '8507')) AND b.person_id IS NOT NULL;---------确保队列中没有人有missing data

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e9e46282-dc3c-44b4-adfd-412004901484"),
    All_concept_set=Input(rid="ri.foundry.main.dataset.80e12731-f476-4faa-aee4-fb64f7a0bdff"),
    censored_cohort_90=Input(rid="ri.foundry.main.dataset.bd2dcaf1-1ada-4843-8d03-994658abd547"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b"),
    person1=Input(rid="ri.foundry.main.dataset.af5e5e91-6eeb-4b14-86df-18d84a5aa010")
)
----this is first verison for table 1,include 15 columns--------------------
-----age into decade, time zero period------------
-----------------------------------------------------------temporary table for medical history--------------------------------------
WITH ConditionCTE AS (
    SELECT
        a.person_id,
        MAX(CASE WHEN c.group_number = 3 THEN 1 ELSE 0 END) AS past_AKI,
        MAX(CASE WHEN c.group_number = 1 THEN 1 ELSE 0 END) AS hypertension,   ----1320106
        MAX(CASE WHEN c.group_number = 2 THEN 1 ELSE 0 END) AS diabetes_mellitus,---278113
        MAX(CASE WHEN c.group_number = 4 THEN 1 ELSE 0 END) AS heart_failure,
        MAX(CASE WHEN c.group_number = 5 THEN 1 ELSE 0 END) AS cardiovascular_disease,
        MAX(CASE WHEN c.group_number = 6 THEN 1 ELSE 0 END) AS obesity
    FROM censored_cohort_90 a
    LEFT JOIN condition_occurrence s ON a.person_id = s.person_id
    LEFT JOIN All_concept_set c ON s.condition_concept_id = c.concept_id
    WHERE s.condition_start_date < time_zero
      AND DATEDIFF(time_zero, s.condition_start_date) < 365
    GROUP BY a.person_id
)

-------------------------------正式表------------------------------------------------------------------------------------------
SELECT DISTINCT a.group_id, a.person_id, a.time_zero, has_AKI, AKI_interval_2,

------------------------gender,不明确的直接扔掉-------------------------------
CASE 
  WHEN b.gender_concept_id = 8532 THEN 'F'
  WHEN b.gender_concept_id = 8507 THEN 'M'
  ELSE NULL 
END AS gender,
-- -------------------准确的年龄列,大于90直接扔掉------------------------------
CAST((2023 - year_of_birth) AS decimal(10,2)) AS age_num,

-- 创建年龄分组列------------------------------------------------------------
CASE
  WHEN year_of_birth IS NOT NULL AND NOT is_age_90_or_older THEN
    CASE
      WHEN CAST(2023 - year_of_birth AS integer) < 30 THEN '<30'
      WHEN CAST(2023 - year_of_birth AS integer) BETWEEN 30 AND 49 THEN '30-49'
      WHEN CAST(2023 - year_of_birth AS integer) BETWEEN 50 AND 64 THEN '50-64'
      WHEN CAST(2023 - year_of_birth AS integer) >= 65 AND CAST(2023 - year_of_birth AS integer) <= 90 THEN '65-90'
      ELSE NULL
    END
  ELSE NULL
END AS age_category,
---race-------------
CASE 
  WHEN race_concept_id = 8527 THEN 'white'
  WHEN race_concept_id = 8516 THEN 'black'
  WHEN race_concept_id = 8515 THEN 'asian' 
  WHEN race_concept_id IS NULL OR race_concept_id = 0 THEN 'no info'
  ELSE 'other'
END AS race,
----ethnicity--------------------
CASE 
  WHEN ethnicity_concept_id = 38003564 THEN 'Not Hispanic or Latino'
  WHEN ethnicity_concept_id = 38003563  THEN 'Hispanic or Latino'
  ELSE 'Unknown'
END AS ethnicity,
--death_date-------------------------------------------------------------------------------
death_date,
--medical history in one year before T0-----------------------------------------------------
       COALESCE(past_AKI, 0) AS past_AKI,
       COALESCE(hypertension, 0) AS hypertension,
       COALESCE(diabetes_mellitus, 0) AS diabetes_mellitus,
       COALESCE(heart_failure, 0) AS heart_failure,
       COALESCE(cardiovascular_disease, 0) AS cardiovascular_disease,
       COALESCE(obesity, 0) AS obesity

FROM censored_cohort_90 a
INNER JOIN person1 b ON a.person_id = b.person_id
LEFT JOIN ConditionCTE c ON a.person_id = c.person_id
WHERE (year_of_birth IS not NULL ) AND ( b.gender_concept_id IN ('8532', '8507')) AND b.person_id IS NOT NULL;---------确保队列中没有人有missing data

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c29948a5-d3f1-473b-be51-9a1b70e73254"),
    queue_group_60=Input(rid="ri.foundry.main.dataset.846368ab-e54b-41c0-8210-5f1f0823180d")
)
----为了添加death censor,删除掉所有missing_death_date的人，除非是有明确的AKI_interval可以用于生存分析的------
SELECT f.*
FROM
(SELECT 
    q.*, 
    CASE 
        WHEN q.death_in_60days = 1 AND q.has_AKI = 0 THEN datediff(q.death_date, q.time_zero)
        ELSE q.AKI_interval
    END AS AKI_interval_2
   
FROM 
    queue_group_60 q
WHERE  
    NOT q.missing_death_date
    AND (
        -- 添加条件去除 AKI_interval_2 小于 0 的数据
        (q.death_in_60days = 1 AND q.has_AKI = 0 AND datediff(q.death_date, q.time_zero) >= 0)
        OR
        (q.death_in_60days != 1 OR q.has_AKI = 1 OR q.AKI_interval >= 0 ) ---为了补足上面的语句------
    )
)f
WHERE AKI_interval_2>= 0 ; -----------二次筛选是为了用简单的方法选出AKI_interval为合理值---------
------------有AKI AKI_start_date的就可以利用时间--------------

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bd2dcaf1-1ada-4843-8d03-994658abd547"),
    quene_group_90=Input(rid="ri.vector.main.execute.459644b9-6b8a-4e6a-a87a-ad26485e4073")
)
----为了添加death censor,删除掉所有missing_death_date的人，除非是有明确的AKI_interval可以用于生存分析的------
SELECT f.*
FROM
(SELECT 
    q.*, 
    CASE 
        WHEN q.death_in_90days = 1 AND q.has_AKI = 0 THEN datediff(q.death_date, q.time_zero)
        ELSE q.AKI_interval
    END AS AKI_interval_2
   
FROM 
    quene_group_90 q
WHERE  
    NOT q.missing_death_date
    AND (
        -- 添加条件去除 AKI_interval_2 小于 0 的数据
        (q.death_in_90days = 1 AND q.has_AKI = 0 AND datediff(q.death_date, q.time_zero) >= 0)
        OR
        (q.death_in_90days != 1 OR q.has_AKI = 1 OR q.AKI_interval >= 0 ) ---为了补足上面的语句------
    )
)f
WHERE AKI_interval_2>= 0 ; -----------二次筛选是为了用简单的方法选出AKI_interval为合理值---------
------------有AKI AKI_start_date的就可以利用时间--------------

@transform_pandas(
    Output(rid="ri.vector.main.execute.459644b9-6b8a-4e6a-a87a-ad26485e4073"),
    Saig90_aki_outcome=Input(rid="ri.foundry.main.dataset.74e11739-c1fe-4d69-bcf7-ec020fb26167"),
    Savg90_aki_outcome=Input(rid="ri.foundry.main.dataset.8c897371-69ff-41d7-a33d-7bfd13307dde"),
    death=Input(rid="ri.foundry.main.dataset.9c6c12b0-8e09-4691-91e4-e5ff3f837e69")
)
-------------- quene group as group information and primary outcome ---------------
------1组为疫苗组,2为感染组
SELECT 1 AS group_id, 'vaccination first' AS group_name,v.person_id,
first_shot AS time_zero,
DATE_ADD(first_shot,90) AS time_end, 
CASE WHEN has_AKI=1 THEN datediff (AKI_start_date,first_shot) ELSE 90 END AS AKI_interval, has_AKI, AKI_start_date,AKI_by,
-----------death1信息---------------------------- 
d.Death_date AS death_date,CASE WHEN death_fact =1 THEN TRUE ELSE FALSE END AS death,
CASE WHEN  datediff (Death_date,first_shot) <= 90 then 1 else 0 end as death_in_90days,
CASE WHEN Death_date IS NULL AND death_fact=1 THEN TRUE ELSE FALSE END AS missing_death_date  ---missing_death_date时如何处理？

FROM Savg90_aki_outcome v
LEFT JOIN (
    --对death数据进行预处理，多个death_date的情况下选择最大日期，保持null值-----
SELECT person_id,MAX(death_date) AS Death_date,1 AS death_fact
FROM death
GROUP BY person_id
) d
ON v.person_id = d.person_id
UNION
SELECT 2 AS group_id,'infection first' AS group_name,i.person_id,first_infection_date AS time_zero,
DATE_ADD(first_infection_date,90) AS time_end, 
CASE WHEN has_AKI=1 THEN datediff (AKI_start_date,first_infection_date) ELSE 90 END 
 AS AKI_interval, has_AKI, AKI_start_date,AKI_by,
-----death1信息-----------------
Death_date AS death_date,CASE WHEN death_fact =1 THEN TRUE ELSE FALSE END AS death,
CASE WHEN  datediff (Death_date,first_infection_date)<=90 then 1 else 0 end as death_in_90days,
CASE WHEN Death_date IS NULL AND death_fact = 1 THEN TRUE ELSE FALSE END AS missing_death_date
FROM Saig90_aki_outcome i
LEFT JOIN(
    SELECT person_id,MAX(death_date) AS Death_date,1 AS death_fact
FROM death
GROUP BY person_id
) d
ON i.person_id = d.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.846368ab-e54b-41c0-8210-5f1f0823180d"),
    Saig60_aki_outcome=Input(rid="ri.foundry.main.dataset.150b1082-da38-4ba5-ba0a-196d508ff11d"),
    Savg60_aki_outcome=Input(rid="ri.foundry.main.dataset.aeec5388-4cfa-4ddf-8eb4-76bc609f93e6"),
    death=Input(rid="ri.foundry.main.dataset.9c6c12b0-8e09-4691-91e4-e5ff3f837e69")
)
-------------- quene group as group information and primary outcome ---------------
------1组为疫苗组,2为感染组
SELECT 1 AS group_id, 'vaccination first' AS group_name,v.person_id,
first_shot AS time_zero,
DATE_ADD(first_shot,60) AS time_end, 
CASE WHEN has_AKI=1 THEN datediff (AKI_start_date,first_shot) ELSE 60 END AS AKI_interval, has_AKI, AKI_start_date,AKI_by,
-----------death1信息---------------------------- 
d.Death_date AS death_date,CASE WHEN death_fact =1 THEN TRUE ELSE FALSE END AS death,
CASE WHEN  datediff (Death_date,first_shot)<=60 then 1 else 0 end as death_in_60days,
CASE WHEN Death_date IS NULL AND death_fact=1 THEN TRUE ELSE FALSE END AS missing_death_date  ---missing_death_date时如何处理？

FROM Savg60_aki_outcome v
LEFT JOIN (
    --对death数据进行预处理，多个death_date的情况下选择最大日期，保持null值-----
SELECT person_id,MAX(death_date) AS Death_date,1 AS death_fact
FROM death
GROUP BY person_id
) d
ON v.person_id = d.person_id
UNION
SELECT 2 AS group_id,'infection first' AS group_name,i.person_id,first_infection_date AS time_zero,
DATE_ADD(first_infection_date,60) AS time_end, 
CASE WHEN has_AKI=1 THEN datediff (AKI_start_date,first_infection_date) ELSE 60 END 
 AS AKI_interval, has_AKI, AKI_start_date,AKI_by,
-----death1信息-----------------
Death_date AS death_date,CASE WHEN death_fact =1 THEN TRUE ELSE FALSE END AS death,
CASE WHEN  datediff (Death_date,first_infection_date)<=60 then 1 else 0 end as death_in_60days,
CASE WHEN Death_date IS NULL AND death_fact = 1 THEN TRUE ELSE FALSE END AS missing_death_date

FROM Saig60_aki_outcome i
LEFT JOIN(
    SELECT person_id,MAX(death_date) AS Death_date,1 AS death_fact
FROM death
GROUP BY person_id
) d
ON i.person_id = d.person_id;

