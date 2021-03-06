rm(list =ls())
library(dplyr)
library(RPostgreSQL)
library(DBI)
library(survey)
library(convey)
library(tidyverse)


#Daten abrufen

pg <- src_postgres(dbname="datacube", host="ineq.wu.ac.at", user='lvineq',password = 'palma', options="-c search_path=silc")

#Daten laden und formatatieren

silc.p <- tbl(pg, 'pp') %>% filter(pb010 %in% c(2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013) & pb020 %in% c("NL")) %>% select(py010g, py020g, py050g, py080g, py090g, py100g, py110g, py120g, py130g, py140g, pb010, pb020, pb030, pb040, pb140, pb150, pb210, pe040, pl020, pl040, pl060, pl100, pl140, pe040, pl200, py010g, px030)
silc.p <- collect(silc.p, n=Inf)

silc.r <- tbl(pg, 'rr') %>% filter(rb010 %in% c(2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013) & rb020 %in% c("NL")) %>% select(rb010, rb020, rb030, rb050, rb080, rb090, rx010, rx030)
silc.r <- collect(silc.r, n=Inf)

silc.d <- tbl(pg, 'dd') %>% filter(db010 %in% c(2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013) & db020 %in% c("NL")) %>% select(db010, db020, db030, db040, db090)
silc.d <- collect(silc.d, n=Inf)

silc.h <- tbl(pg, 'hh') %>% filter(hb010 %in% c(2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013) & hb020 %in% c("NL")) %>% select(hb010, hb020, hb030, hy010, hy020, hx050, hx060, hy110g, hy040g, hy050g, hy060g, hy070g, hy080g, hy090g, hy120g, hy130g, hy140g)
silc.h <- collect(silc.h, n=Inf)

# 2. Download c[YY]p tables 2007 - 2013:
# Download c[YY]p tables from 2007 - 2013

c07p <- tbl(pg, "c07p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030,                                                                  py021g) %>% collect(n = Inf)

c08p <- tbl(pg, "c08p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030, 
                                                                  py021g) %>% collect(n = Inf)

c09p <- tbl(pg, "c09p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030, 
                                                                  py021g) %>% collect(n = Inf)

c10p <- tbl(pg, "c10p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030, 
                                                                  py021g) %>% collect(n = Inf)

c11p <- tbl(pg, "c11p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030, 
                                                                  py021g) %>% collect(n = Inf)

c12p <- tbl(pg, "c12p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030, 
                                                                  py021g) %>% collect(n = Inf)

c13p <- tbl(pg, "c13p") %>% filter(pb020 %in% c("NL")) %>% select(pb010, pb030, 
                                                                  py021g) %>% collect(n = Inf)
cxxp <- bind_rows(c07p, c08p, c09p, c10p, c11p, c12p, c13p)
rm(c07p, c08p, c09p, c10p, c11p, c12p, c13p)


# merge cxxp with silc.p to get py021g variable for 2007-2013

silc.p <- left_join(silc.p, cxxp %>% select(py021g, pb010, pb030))

################# data for 2014-2017 ###########################

### 3.1 prepare cyyp:
c14p <- tbl(pg, "c14p") %>% filter(pb020 %in% c("NL")) %>% 
  select(py010g, py050g, py080g, py090g, py100g, py110g, py120g, py130g, py140g, pb010, pb020, pb030, pb040, pb140, pb150, pb210, pe040, pl020, pl040, pl060, pl100, pl140, pe040, pl200, py010g, px030) %>% collect(n = Inf)

c15p <- tbl(pg, "c15p") %>% filter(pb020 %in% c("NL")) %>% 
  select(py010g, py050g, py080g, py090g, py100g, py110g, py120g, py130g, py140g, pb010, pb020, pb030, pb040, pb140, pb150, pb210, pe040, pl020, pl040, pl060, pl100, pl140, pe040, pl200, py010g, px030) %>% collect(n = Inf)

c16p <- tbl(pg, "c16p") %>% filter(pb020 %in% c("NL")) %>% 
  select(py010g, py050g, py080g, py090g, py100g, py110g, py120g, py130g, py140g, pb010, pb020, pb030, pb040, pb140, pb150, pb210, pe040, pl020, pl040, pl060, pl100, pl140, pe040, pl200, py010g, px030) %>% collect(n = Inf)

cyyp <- bind_rows(c14p, c15p, c16p)
rm(c14p, c15p, c16p)

# merge data to silc.p
silc.p <- bind_rows(silc.p, cyyp)

### 3.2 prepare cyyh:
c14h <- tbl(pg, "c14h") %>%
  filter(hb020 %in% c("NL")) %>%
  select(hb010, hb020, hb030, hy010, hy020, hy040g, hy050g, hy060g, hy070g, hy080g, 
         hy090g, hy110g, hy120g, hy130g, hy140g, hx010, hx050) %>%
  collect(n = Inf)

c15h <- tbl(pg, "c15h") %>%
  filter(hb020 %in% c("NL")) %>%
  select(hb010, hb020, hb030, hy010, hy020, hy040g, hy050g, hy060g, hy070g, hy080g, 
         hy090g, hy110g, hy120g, hy130g, hy140g, hx010, hx050) %>%
  collect(n = Inf)

c16h <- tbl(pg, "c16h") %>%
  filter(hb020 %in% c("NL")) %>%
  select(hb010, hb020, hb030, hy010, hy020, hy040g, hy050g, hy060g, hy070g, hy080g, 
         hy090g, hy110g, hy120g, hy130g, hy140g, hx010, hx050) %>%
  collect(n = Inf)

cyyh <- bind_rows(c14h, c15h, c16h)
rm(c14h, c15h, c16h)

# merge data to silc.h
silc.h <- bind_rows(silc.h, cyyh)

### 3.3 prepare cyyd:

c14d <- tbl(pg, "c14d") %>%
  filter(db020 %in% c("NL")) %>%
  select(db010, db020, db030, db040, db090) %>%
  collect(n = Inf)

c15d <- tbl(pg, "c15d") %>%
  filter(db020 %in% c("NL")) %>%
  select(db010, db020, db030, db040, db090) %>%
  collect(n = Inf)

c16d <- tbl(pg, "c16d") %>%
  filter(db020 %in% c("NL")) %>%
  select(db010, db020, db030, db040, db090) %>%
  collect(n = Inf)

cyyd <- bind_rows(c14d, c15d, c16d)
rm(c14d, c15d, c16d)

# merge data to silc.d
silc.d <- bind_rows(silc.d, cyyd)

### 3.4 prepare cyyr:

c14r <- tbl(pg, "c14r") %>% 
  filter(rb020 %in% c("NL")) %>%
  select(rb010, rb020, rb030, rb050, rb080, rb090, rx010, rx030) %>%
  collect(n = Inf)

c15r <- tbl(pg, "c15r") %>% 
  filter(rb020 %in% c("NL")) %>%
  select(rb010, rb020, rb030, rb050, rb080, rb090, rx010, rx030) %>%
  collect(n = Inf)

c16r <- tbl(pg, "c16r") %>% 
  filter(rb020 %in% c("NL")) %>%
  select(rb010, rb020, rb030, rb050, rb080, rb090, rx010, rx030) %>%
  collect(n = Inf)

cyyr <- bind_rows(c14r, c15r, c16r)
rm(c14r, c15r, c16r)

# merge data to silc.r
silc.r <- bind_rows(silc.r, cyyr)


######################### 4. Merging ######################################

#Create unique IDs for merging
silc.p <- silc.p %>% mutate(id_h = paste0(pb020, px030), 
                            id_p = paste0(pb020, pb030))

silc.h <- silc.h %>% mutate(id_h = paste0(hb020, hb030))

silc.d <- silc.d %>% mutate(id_h = paste0(db020, db030))

silc.r <- silc.r %>% mutate(id_h = paste0(rb020, rx030), 
                            id_p = paste0(rb020, rb030)) 

# Merge the datasets
silc.pd <- left_join(silc.p, silc.d %>% select(id_h, db010, db020, db040, db090)
                     , by = c('id_h'='id_h', 'pb010'='db010'))

silc.hd <- left_join(silc.h, silc.d %>% select(id_h, db010, db020, db040, db090)
                     , by = c('id_h' = 'id_h', 'hb010'='db010'))

silc.rp <- left_join(silc.r, silc.p, by= c('id_p' = 'id_p', 'rb010' = 'pb010'))

rm(cxxp, cyyp, cyyh, cyyd, cyyr)

#create 'car' variable for later use, combine py020g & py021g

int1 <- seq(2004,2006,1)
int2 <- seq(2007,2016,1)
df1 <- silc.pd %>% filter(pb010 %in% int1)
df2 <- silc.pd %>% filter(pb010 %in% int2)
df1$car <- df1$py020g
df2$car <- df2$py021g

silc.pd <- bind_rows(df1,df2)

df3 <- silc.rp %>% filter(rb010 %in% int1)
df4 <- silc.rp %>% filter(rb010 %in% int2)
df3$car <- df3$py020g
df4$car <- df4$py021g

silc.rp <- bind_rows(df3,df4)

rm(int1,int2,df1,df2, df3, df4)

#verbinden von P und h dataset
silc.rph <- left_join(silc.rp, silc.hd, by = c('id_h.x' = 'id_h', 
                                              'rb010' = 'hb010' ))
#Methode P1 Eurostat

# 1.2  check for NA's and if we can set them to 0:
# diff <- nrow(silc.r) - nrow(silc.p)
summary(is.na(silc.rph))

# now only Persons not in p but in r (age<16) are NA's. so its fine to set 0

silc.rph[is.na(silc.rph)] <- 0

#Pre-tax factor income

#Summe personal income

# summe Einkommen aus Arbeit :p_inc
silc.rph <- silc.rph %>% mutate(p_inc = py010g + car + py050g + py080g) 

#Haushaltseinkommen: h_inc
silc.rph <- silc.rph %>% mutate(h_inc = hy040g + hy080g + hy090g + hy110g)

silc.rph <- silc.rph %>% group_by(rb010, id_h.x) %>% mutate(sum_p_inc = sum(p_inc))

#Pre-tax factor income [income_pti1]
silc.rph <- silc.rph %>% mutate(income_pti1 = (sum_p_inc + h_inc)/hx050)



#Pre-tax national income

#Pensionen + Arbeitslosengeld
silc.rph <- silc.rph %>% mutate(benefits = py090g + py100g)
silc.rph <- silc.rph %>% group_by(id_h.x, rb010) %>% mutate(sum_benefits = sum(benefits))


#Pre-tax national income [income_ptni1]
silc.rph <- silc.rph %>% mutate(income_ptni1 =( income_pti1 +
                                                  sum_benefits/hx050))

#Post-tax disposable income


#sum(transfers)
silc.rph <- silc.rph %>% mutate(ptransfers = py110g + py120g + py130g + py140g)
silc.rph <- silc.rph %>% mutate(htransfers = hy050g + hy060g + hy070g + hy080g)

silc.rph <- silc.rph %>% group_by(id_h.x, rb010) %>% mutate(sum_ptransfers = sum(ptransfers))
#sum(taxes)
silc.rph <- silc.rph %>% mutate(taxes = hy120g + hy130g + hy140g)

#Post-tax disposable income [income_ptdi1]
silc.rph <- silc.rph %>% mutate(income_ptdi1 = income_ptni1 + (ptransfers + 
                                                                 htransfers - taxes)/hx050)

#überprüfen ob income_p13 = hy020a

silc.rph$hy020a <- silc.rph$hy020/silc.rph$hx050
#(silc.rph$income_ptni1)
#summary(silc.rph$hy020a)

summary(silc.rph$hy020a==silc.rph$income_ptdi1)
# 
#silc.rph$income_p13_round0 <- round(silc.rph$income_p13, digits = 0)
#silc.rph$hy020a_round0 <- round(silc.rph$hy020a, digits = 0)
#summary(silc.rph$hy020a_round0==silc.rph$income_p13_round0)
## Not true: Grund? sum_p_inc wirkt falsch?

#P2 (wid.world)

#nur über 20 jährige filtern
n<-add_count(silc.rph$id_h.y)
silc.rph2 <- silc.rph %>% filter(rx010 >= 20) %>% add_count(id_h.y) %>% 
  rename(n_hh = n)

#Pre-tax factor income 

silc.rph2 <- silc.rph2 %>% mutate(income_ptfi2 = p_inc + h_inc/n_hh)

#Pre-tax national income

silc.rph2 <- silc.rph2 %>% mutate(income_ptni2 = income_ptfi2 + benefits)


#Post-tax disposable income

silc.rph2 <- silc.rph2 %>% mutate(income_ptdi2 = income_ptni2 + ptransfers +
                                    htransfers/n_hh - taxes/n_hh)

#Creating Survey Objects

silc.rph <- silc.rph %>% filter(income_pti1 >0, income_ptni1 > 0, income_ptdi1 > 0,
                                income_ptfi2 >0, income_ptni2 > 0, income_ptdi2 > 0)
silc.rph2 <- silc.rph2 %>% filter(income_pti1 >0, income_ptni1 > 0, income_ptdi1 > 0,
                                  income_ptfi2 >0, income_ptni2 > 0, income_ptdi2 > 0)

P1.svy <- svydesign(ids =  ~ id_p,
                    strata = ~rb020,
                    weights = ~rb050,
                    data = silc.rph) %>% convey_prep()

P2.svy <- svydesign(ids = ~id_p,
                    strata = ~rb020,
                    weights = ~rb050,
                    data = silc.rph2) %>% convey_prep()


# pre-tax factor income
mean_pti1 <- svymean(~income_pti1, P1.svy)
median_pti1 <- svyquantile(~income_pti1, P1.svy, quantiles = c(0.5))
gini_pti1 <- svygini(~income_pti1, P1.svy)
P8020_pti1 <- svyqsr(~income_pti1, P1.svy, 0.2, 0.8)
# 
top10_pti1 <- svytotal(~income_pti1, subset(P1.svy, income_pti1 >= 
                                              as.numeric(svyquantile(~income_pti1, P1.svy, 
                                                                     quantile = 0.9)))) / svytotal(~income_pti1, P1.svy)

#  pre-tax national income
mean_ptni1 <- svymean(~income_ptni1, P1.svy)
median_ptni1 <- svyquantile(~income_ptni1, P1.svy, quantiles = c(0.5))
gini_ptni1 <- svygini(~income_ptni1, P1.svy)
P8020_ptni1 <- svyqsr(~income_ptni1, P1.svy, 0.2, 0.8)

top10_ptni1 <- svytotal(~income_ptni1, subset(P1.svy, income_ptni1 >= 
                                                as.numeric(svyquantile(~income_ptni1, P1.svy, 
                                                                       quantile = 0.9)))) / svytotal(~income_ptni1, P1.svy)
# post-tax disposable income
mean_ptdi1 <- svymean(~income_ptdi1, P1.svy)
median_ptdi1 <- svyquantile(~income_ptdi1, P1.svy, quantiles = c(0.5))
gini_ptdi1 <- svygini(~income_ptdi1, P1.svy)
P8020_ptdi1 <- svyqsr(~income_ptdi1, P1.svy, 0.2, 0.8)
top10_ptdi1 <- svytotal(~income_ptdi1, subset(P1.svy, income_ptdi1 >= 
                                              as.numeric(svyquantile(~income_ptdi1, P1.svy, 
                                                                     quantile = 0.9)))) / svytotal(~income_ptdi1, P1.svy)
# Var 2
# pre-tax factor income
mean_ptfi2 <- svymean(~income_ptfi2, P2.svy)
median_ptfi2 <- svyquantile(~income_ptfi2, P2.svy, quantiles = c(0.5))
gini_ptfi2 <- svygini(~income_ptfi2, P2.svy)
P8020_ptfi2 <- svyqsr(~income_ptfi2, P2.svy, 0.2, 0.8)
top10_ptfi2 <- svytotal(~income_ptfi2, subset(P2.svy, income_ptfi2 >= 
                                              as.numeric(svyquantile(~income_ptfi2, P2.svy, 
                                                                     quantile = 0.9)))) / svytotal(~income_ptfi2, P2.svy)
# pre-tax national income
mean_ptni2 <- svymean(~income_ptni2, P2.svy)
median_ptni2 <- svyquantile(~income_ptni2, P2.svy, quantiles = c(0.5))
gini_ptni2 <- svygini(~income_ptni2, P2.svy)
P8020_ptni2 <- svyqsr(~income_ptni2, P2.svy, 0.2, 0.8)
top10_ptni2 <- svytotal(~income_ptni2, subset(P2.svy, income_ptni2 >= 
                                                as.numeric(svyquantile(~income_ptni2, P2.svy, 
                                                                       quantile = 0.9))))/ svytotal(~income_ptni2, P2.svy)
# post-tax disposable income
mean_ptdi2 <- svymean(~income_ptdi2, P2.svy)
median_ptdi2 <- svyquantile(~income_ptdi2, P2.svy, quantiles = c(0.5))
gini_ptdi2 <- svygini(~income_ptdi2, P2.svy)
P8020_ptdi2 <- svyqsr(~income_ptdi2, P2.svy, 0.2, 0.8)
top10_ptdi2 <- svytotal(~income_ptdi2, subset(P2.svy, income_ptdi2 >= 
                                                as.numeric(svyquantile(~income_ptdi2, P2.svy, 
                                                                       quantile = 0.9))))/ svytotal(~income_ptdi2, P2.svy)


#### grouped by years

mean_pti1 <- svyby(~income_pti1, ~rb010, P1.svy, svymean)
median_pti1 <- svyby(~income_pti1, ~rb010, P1.svy, svyquantile, quantile = 0.5)
gini_pti1 <- svyby(~income_pti1, ~rb010, P1.svy, svygini)
