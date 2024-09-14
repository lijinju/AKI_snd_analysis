

@transform_pandas(
    Output(rid="ri.vector.main.execute.3b68daf1-6a91-4f91-8a05-925d1ad0521b"),
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

