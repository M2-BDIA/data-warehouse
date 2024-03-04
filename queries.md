# Analyses à Cleveland

## Résultats globaux

### Carte des commerces de Cleveland

### Le nombre de commerces à Cleveland 
```sql
select count(*)
from BUSINESS
where "city" = 'cleveland'
```

### Le nombre de visites total des commerces de Cleveland 
```sql
select trunc(sum("nb_visits"))
from BUSINESS
where "city" = 'cleveland'
```

### Le nombre d'avis total des commerces de Cleveland
```sql
select trunc(sum("review_count")) + trunc(sum("nb_tips"))
from BUSINESS
where "city" = 'cleveland'
```

### La moyenne des avis (review) à Cleveland
```sql
select avg("avg_stars")
from BUSINESS
where "city" = 'cleveland'
```

### La moyenne du nombre d'avis des commerces de Cleveland
```sql
select avg("review_count")
from BUSINESS
where "city" = 'cleveland'
```

### La moyenne du nombre de visites dans les commerces de Cleveland
```sql
select avg("nb_visits")
from BUSINESS
where "city" = 'cleveland'
```

### Le nombre de commerces par type à Cleveland
```sql
select NVL("main_category", 'non communiqué') as "catégorie", count(*) as "nombre de commerces"
from business
where "city" = 'cleveland'
group by "main_category"
order by count(*) desc
```

### Le nombre de commerces par quartier (code postal) à Cleveland
```sql
select "postal_code", count(*) as nb_business
from business
where "city" = 'cleveland'
group by "postal_code"
order by nb_business desc
```

### Le nombre de visites par mois et année dans les commerces de Cleveland
```sql
select d."year" as "année", d."month" as "mois", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
group by CUBE(d."year", d."month")
```

### Nombre de visites par année dans des commerces de Cleveland
```sql
select d."year" as "année", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
group by d."year"
order by d."year"
```

### Evolution du nombre de visites par année et type de commerce à Cleveland
```sql
select d."year" as "année", NVL(b."main_category", 'non communiqué') as "catégorie", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
group by d."year", b."main_category"
order by sum(v."nb_visits") desc
```

### Evolution du nombre d'avis pour les commerces de Cleveland
```sql
select d."year" as "année", count(distinct r."review_id") as "nombre d'avis"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
group by d."year"
```

### Evolution du nombre d'avis par année et type de commerce à Cleveland
```sql
select d."year" as "année", NVL(b."main_category", 'non communiqué') as "catégorie", count(distinct r."review_id") as "nombre d'avis"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
group by d."year", b."main_category"
order by count(distinct r."review_id") desc
```

### Evolution par année des notes des commerces de Cleveland
```sql
select d."year" as "année", avg(r."stars") as "note moyenne"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
group by d."year"
```

### Le nombre de visites par année et type de commerce à Cleveland
```sql
select d."year" as "année", b."main_category" as "catégorie", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
group by cube(d."year", b."main_category")
order by sum(v."nb_visits") desc
```

### Le nombre de visites par année et quartier
```sql
select d."year" as "année", b."postal_code", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
group by cube(d."year", b."postal_code")
```

### Répartition des types de commerces ouverts le dimanche à Cleveland
```sql
select NVL("main_category", 'non communiqué') as "main_category", count(*) as "nombre de commerces"
from BUSINESS
where "city" = 'cleveland'
and "is_open_sunday" = '1'
group by "main_category"
order by count(*) desc
```

### Le nombre d'avis par année et type de commerce à Cleveland
```sql
select d."year" as "année", b."main_category" as "catégorie", count(r."review_id") as "nombre de reviews"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
group by cube(d."year", b."main_category")
```

### Le nombre d'avis par année et quartier à Cleveland
```sql
select d."year" as "année", b."postal_code", count(r."review_id") as "nombre de reviews"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
group by cube(d."year", b."postal_code")
```

### La moyenne des avis par quartier
```sql
select "postal_code", avg("avg_stars") as "note moyenne"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" is not null
group by "postal_code"
order by "postal_code"
```

### Les 10 restaurants les mieux notés de Cleveland
```sql
select "name", "review_count", "avg_stars", "latitude", "longitude"
from business
where "city" = 'cleveland'
and "main_category" = 'restaurant'
order by "avg_stars" desc, "review_count" desc
fetch first 10 rows only
```

### Les 10 commerces de Cleveland avec le plus de visites
```sql
select "name", "main_category", "postal_code", "nb_visits", "latitude", "longitude"
from business
where "city" = 'cleveland'
order by "nb_visits" desc
fetch first 10 rows only
```

### Evolution de la note moyenne des commerces de Cleveland par année et type de commerce
```sql
select d."year" as "année", NVL(b."main_category", 'non communiqué') as "main_category", avg(r."stars") as "note moyenne", count(r."review_id") as "nombre de notes"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
group by d."year", b."main_category"
```

### La moyenne des notes par type de commerce à Cleveland
```sql
select NVL("main_category", 'non communiqué') as "categorie", avg("avg_stars") as "note moyenne"
from BUSINESS
where "city" = 'cleveland'
group by "main_category"
order by "categorie"
```

### La moyenne des notes par type de commerce pondérée par le nombre d'avis à Cleveland
```sql
select "main_category" as "categorie", avg("avg_stars") as "note moyenne", sum("review_count") as "nombre d'avis"
from business
where "city" = 'cleveland'
and "main_category" is not null
group by "main_category"
order by "main_category"
```


## Résultats par catégorie de commerces

### Le nombre de commerces pour une catégorie en particulier à Cleveland
```sql
select count(*) as "Nombre de commerces"
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
```

### Nombre de visites pour un type de commerce en particulier à Cleveland
```sql
select sum("nb_visits") as "Nombre de visites"
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
```

### Le nombre d'avis pour un type de commerce en particulier à Cleveland
```sql
select sum("review_count") as "Nombre d'avis"
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
```

### La note moyenne d'un type de commerce en particulier à Cleveland
```sql
select avg("avg_stars") as "Note moyenne"
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
```

### Le nombre de visites moyen pour un type de commerce en particulier à Cleveland
```sql
select avg("nb_visits") as "Nombre de visites"
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
```

### Carte d'un type de commerce en particulier à Cleveland
```sql
select *
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
```

### La répartition du nombre de commerces par quartier pour un type de commerce en particulier à Cleveland
```sql
select "postal_code", count(*) as "Nombre de commerces"
from BUSINESS
where "city" = 'cleveland'
and "main_category" = {{category}}
group by "postal_code"
order by count(*) desc
```

### Le quartier dans lequel le nombre de visites pour un type de commerce en particulier est le plus élevé à Cleveland
```sql
select "postal_code"
from (
    select "main_category", "postal_code", sum("nb_visits") as nb_visits, rank() over (partition by "main_category" order by sum("nb_visits") desc) as rank
    from business
    where "city" = 'cleveland'
    and "main_category" = {{category}}
    group by "main_category", "postal_code"
)
where rank = 1
```

### Evolution du nombre de visites par année pour un type de commercer en particulier à Cleveland
```sql
select d."year" as "année", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "main_category" = {{category}}
group by d."year"
```

### Evolution du nombre de visites par année et quartier pour un type de commercer en particulier à Cleveland
```sql
select d."year" as "année", b."postal_code", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "main_category" = {{category}}
group by d."year", b."postal_code"
```

### Le nombre de visites par année et quartier pour un type de commerce en particulier à Cleveland (avec cube)
```sql
select d."year" as "année", b."postal_code", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "main_category" = {{category}}
group by CUBE(d."year", b."postal_code")
```

### Le nombre de visites par mois et année dans un type de commerce en particulier à Cleveland
```sql
select d."year" as "année", d."month" as "mois", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "main_category" = {{category}}
group by CUBE(d."year", d."month")
```

### Evolution du nombre d'avis par an pour un type de commerce en particulier à Cleveland
```sql
select d."year" as "année", count(r."review_id") as "nombre d'avis"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "main_category" = {{category}}
group by d."year"
```

### Evolution de la notation par an pour un type de commerce en particulier à Cleveland
```sql
select d."year" as "année", avg(r."stars") as "note moyenne"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "main_category" = {{category}}
group by d."year"
```


## Résultats par quartier

### Le type de commerce ayant le plus de visites pour un quartier en particulier de Cleveland
```sql
select "main_category"
from (
    select "postal_code", "main_category", sum("nb_visits") as nb_visits, rank() over (partition by "postal_code" order by sum("nb_visits") desc) as rank
    from business
    where "postal_code" = {{postal_code}}
    and "main_category" is not null
    group by "postal_code", "main_category"
)
where rank = 1
order by "postal_code"
```

### Le nombre de commerces pour un quartier en particulier à Cleveland
```sql
select count(*)
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre de visites moyen pour un quartier en particulier à Cleveland
```sql
select avg("nb_visits") as "nombre de visites moyen"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre d'avis moyen pour un quartier en particulier à Cleveland
```sql
select avg("review_count") as "nombre d'avis moyen"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre d'avis pour un quartier en particulier à Cleveland
```sql
select sum("review_count") as "nombre d'avis"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre de visites pour un quartier particulier de Cleveland
```sql
select sum("nb_visits") as "nombre de commerces"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Les commerces d'un quartier de Cleveland
```sql
select *
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre de commerces par catégorie pour un code postal à Cleveland
```sql
select NVL("main_category", 'non communiqué') as "catégorie", count(*) as "nombre de commerces"
from business
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by "main_category"
order by count(*) desc
```

### Les 10 restaurants les mieux notés d'un quartier de Cleveland
```sql
select "name", "review_count", "avg_stars", "latitude", "longitude"
from business
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
and "main_category" = 'restaurant'
order by "avg_stars" desc, "review_count" desc
fetch first 10 rows only
```

### Les 10 commerces d'un quartier de Cleveland avec le plus de visites
```sql
select "name", "main_category", "nb_visits", "latitude", "longitude"
from business
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
order by "nb_visits" desc
fetch first 10 rows only
```

### Evolution du nombre de visites par année d'un quartier en particulier à Cleveland
```sql
select d."year" as "année", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by d."year"
order by d."year"
```

### Le nombre de visites par année et type de commerce d'un quartier en particulier de Cleveland
```sql
select d."year" as "année", b."main_category" as "catégorie", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by cube(d."year", b."main_category")
```

### Evolution du nombre d'avis par année d'un quartier en particulier à Cleveland
```sql
select d."year" as "année", count(distinct r."review_id") as "nombre d'avis"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by d."year"
```

### Le nombre d'avis par année et type de commerce pour un quartier en particulier de Cleveland
```sql
select d."year" as "année", b."main_category" as "catégorie", count(r."review_id") as "nombre de reviews"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by cube(d."year", b."main_category")
```

### Evolution des notes d'un quartier en particulier de Cleveland
```sql
select d."year" as "année", avg(r."stars") as "note moyenne"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by d."year"
```

### Evolution de la note moyenne des commerces d'un quartier de Cleveland par type de commerce
```sql
select d."year" as "année", NVL(b."main_category", 'non communiqué') as "main_category", avg(r."stars") as "note moyenne", count(r."review_id") as "nombre de notes"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by d."year", b."main_category"
```

### La moyenne des avis par catégorie pour un quartier en particulier de Cleveland
```sql
select NVL("main_category", 'non communiqué') as "categorie", avg("avg_stars") as "note moyenne"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
group by "main_category"
```

### La moyenne des notes par type de catégorie pondérée par le nombre d'avis dans un quartier de Cleveland
```sql
select "main_category" as "categorie", avg("avg_stars") as "note moyenne", sum("review_count") as "nombre d'avis"
from business
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
and "main_category" is not null
group by "main_category"
```


## Comparaison de quartiers

### Le nombre d'avis pour un quartier en particulier à Cleveland
```sql
select sum("review_count") as "nombre d'avis"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre d'avis moyen par commerce pour un quartier en particulier à Cleveland
```sql
select avg("review_count") as "nombre d'avis moyen"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre de visites moyen pour un quartier en particulier à Cleveland
```sql
select avg("nb_visits") as "nombre de visites moyen"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre d'avis pour un quartier en particulier à Cleveland
```sql
select sum("review_count") as "nombre d'avis"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre d'avis moyen pour un quartier en particulier à Cleveland
```sql
select avg("review_count") as "nombre d'avis moyen"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Le nombre de visites moyen pour un quartier en particulier à Cleveland
```sql
select avg("nb_visits") as "nombre de visites moyen"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" = {{postal_code}}
```

### Comparaison de l'évolution du nombre de visites par année de deux quartiers de Cleveland
```sql
select d."year" as "année", "postal_code", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
group by d."year", "postal_code"
order by d."year"
```

### Comparaison de l'évolution du nombre d'avis par année de deux quartiers de Cleveland
```sql
select d."year" as "année", "postal_code", count(distinct r."review_id") as "nombre d'avis"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
group by d."year", "postal_code"
```

### Comparaison de l'évolution de la note moyenne par année de deux quartiers de Cleveland
```sql
select d."year" as "année", "postal_code", avg(r."stars") as "note moyenne"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
group by d."year", "postal_code"
```

### Comparaison de la moyenne des avis par catégorie de deux quartiers de Cleveland
```sql
select NVL("main_category", 'non communiqué') as "categorie", "postal_code", avg("avg_stars") as "note moyenne"
from BUSINESS
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
group by "main_category", "postal_code"
```

### Comparaison du nombre de commerces par catégorie entre deux quartiers de cleveland
```sql
select NVL("main_category", 'non communiqué') as "catégorie", "postal_code", count(*) as "nombre de commerces"
from business
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
group by "main_category", "postal_code"
order by count(*) desc
```

### Comparaison du nombre de visites de commerces par catégorie entre deux quartiers de Cleveland
```sql
select NVL("main_category", 'non communiqué') as "catégorie", "postal_code", sum(v."nb_visits") as "nombre de visites"
from (BUSINESS b inner join VISIT v on b."business_id" = v."business_id")
    inner join DATE_DETAIL d on d."date_id" = v."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[ and d."year" = {{year}} ]]
group by "main_category", "postal_code"
```

### Comparaison du nombre d'avis par catégorie pour une année en particulier de deux quartiers de Cleveland
```sql
select NVL("main_category", 'non communiqué') as "catégorie", "postal_code", count(r."review_id") as "nombre d'avis"
from (BUSINESS b inner join REVIEW r on b."business_id" = r."business_id")
    inner join DATE_DETAIL d on d."date_id" = r."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[and "year" = {{annee}}]]
group by "main_category", "postal_code"
```

### Comparaison du nombre de visites de commerces par catégorie entre deux quartiers de Cleveland
```sql
select NVL("main_category", 'non communiqué') as "catégorie", "postal_code", sum(v."nb_visits") as "nombre de visites"
from (BUSINESS b inner join VISIT v on b."business_id" = v."business_id")
    inner join DATE_DETAIL d on d."date_id" = v."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[ and d."year" = {{year}} ]]
group by "main_category", "postal_code"
```

### Comparaison de la moyenne des avis par catégorie pour une année en particulier de deux quartiers de Cleveland
```sql
select NVL("main_category", 'non communiqué') as "categorie", "postal_code", avg(r."stars") as "note moyenne", count(r."review_id") as "nombre notes"
from (BUSINESS b inner join REVIEW r on b."business_id" = r."business_id")
    inner join DATE_DETAIL d on d."date_id" = r."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[ and "year" = {{annee}} ]]
group by "main_category", "postal_code"
```

### Comparaison de l'évolution du nombre de visites par mois de deux quartiers de Cleveland
```sql
select d."month" as "mois", "postal_code", sum(v."nb_visits") as "nombre de visites"
from (VISIT v inner join BUSINESS b 
on v."business_id" = b."business_id")
inner join DATE_DETAIL d 
on v."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[ and d."year" = {{year}} ]]
group by d."month", "postal_code"
order by d."month"
```

### Comparaison de l'évolution du nombre d'avis par mois de deux quartiers de Cleveland
```sql
select d."month" as "mois", "postal_code", count(distinct r."review_id") as "nombre d'avis"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[ and d."year" = {{year}} ]]
group by d."month", "postal_code"
```

### Comparaison de l'évolution de la note moyenne par mois de deux quartiers de Cleveland
```sql
select d."month" as "mois", "postal_code", avg(r."stars") as "note moyenne"
from (REVIEW r inner join BUSINESS b 
on r."business_id" = b."business_id")
inner join DATE_DETAIL d 
on r."date_id" = d."date_id"
where "city" = 'cleveland'
and "postal_code" IN ({{postal_code1}}, {{postal_code2}})
[[ and d."year" = {{year}} ]]
group by d."month", "postal_code"
```
