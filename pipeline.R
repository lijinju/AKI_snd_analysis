

@transform_pandas(
    Output(rid="ri.vector.main.execute.c22347d6-8b87-462b-b7cc-36a272cc7fd3")
)
HR_plot <- function() {
    
    library(forestplot)

    col1 <- c('30-Day Observational Period', "Defined by diagnostic Code", "Defined by Serum Creatinine Change", "Defined by both method", "60-Day Observational Period", "90-Day Observational Period", "Phase 1:Alpha", "Phase 2:Delta", "Phase 3: Omicron")
    col2 <- c(19621, 8270, 16103, 4752, 33240, 44000, 5304, 9852, 4465)
    col3 <- c(2933598, 2944949, 2937116, 2948467, 2762654, 2732300, 997664, 1607247, 328687)
    col4 <- c(176731, 91358, 153783, 68410, 174007, 180092, 33881, 65215, 77635)
    col5 <- c(3440071, 3525444, 3463019, 3548392, 3144726, 3075798, 679466, 1215690, 1544915)
    col6 <- c(10.31, 6.26, 5.00, 12.61, 10.71, 16.19, 14.26, 12.50, 4.55)
    col7 <- c(10.16, 6.19,4.95,12.33,10.54,15.72, 13.85,12.23,4.42 )
    col8 <- c(10.47,6.33, 5.05, 12.90, 10.89, 16.68, 14.68,12.77,4.69)

    data <- data.frame(
    "Column1" = col1,
    "Column2" = col2,
    "Column3" = col3,
    "Column4" = col4,
    "Column5" = col5,
    "Column6" = col6,
    "Column7" = col7,
    "Column8" = col8
     )  

    return(data)

    tabletext <- cbind(c("Outcomes","\n",data$Column1),
                   c("Vaccination Group\nNo. of AKI patients(N)","\n",data$Column2),
                   c("Vaccination Group\nNo of NOT AKI patients(N)","\n",data$Column3),
                   c("Infection Group\nNo. of AKI patients(N)","\n",data$Column4 ),
                   c("Infection Group\nNo. of NOT AKI patients(N)","\n",data$Column5))
    
    plot <- forestplot(
    labeltext=tabletext,
    mean =c(NA,NA,data$Column6), 
    lower =c(NA,NA,data$Column7), 
    upper =c(NA,NA,data$Column8),
    graph.pos=2,
    title = "Harzad ratio of different outcomes",
    xlab = "HR",
    xticks = c( 3, 5, 7,9, 12),
    new_page = TRUE,
    zero = 3,
    boxsize = 0.15,
    graphwidth = unit(.2, "npc"), 
    txt_gp = fpTxtGp(                # 文本样式的设置
    label = gpar(cex = 0.7),       # 标签的大小
    ticks = gpar(cex = 0.7),         # 刻度标记的大小
    xlab = gpar(cex = 0.7),        # x轴标签的大小
    title = gpar(cex = 1.2),       # 标题的大小
    cex = 0.75             #文本缩放比例
     ),
    ci.vertices = TRUE,
    fn.ci_norm = "fpDrawDiamondCI",
    lwd.ci=2,
    box = gpar(lty = 2, col = "lightgray"),
    col=fpColors(box="black", lines="#1c61b6", zero = "gray50"),
    hrzl_lines = list(               # 水平线样式的设置
    "1" = gpar(lty = 1, lwd = 2),  # 均值线
    "2" = gpar(lty = 2),           # 下限和上限之间的虚线
    "12" = gpar(lwd = 2, lty = 1, columns = c(3:6)) # 下限和上限线
         ))
         
   print(plot)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.ddf67bd2-c537-4340-bfcc-9c789011b696"),
    censored_cohort_60=Input(rid="ri.foundry.main.dataset.c29948a5-d3f1-473b-be51-9a1b70e73254")
)
RR_SA60 <- function(censored_cohort_60) {
    
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.1aaa926b-5a33-4808-9bf7-7a6f7bee11e3"),
    SA60TABLE1=Input(rid="ri.foundry.main.dataset.cc92d73d-a942-4f57-bc8a-14ffc8f1c177")
)
SA60KM <- function(SA60TABLE1) {
    library(survival)
    library(survminer)

    SA60TABLE1 <- within(SA60TABLE1, {group_id <- factor(group_id, labels = c('vaccination', 'infection'))})

    KM <- survfit(Surv(AKI_interval_2, has_AKI ) ~  group_id, data = SA60TABLE1)    #  AKI_by_code as outcome

    ab <- summary(KM, times = c(0,10,20,30,40,50,60))

    plot <- ggsurvplot(
        KM,
        conf.int = TRUE,  
        ylab = expression(bold("Probability of NOT developing AKI")),
        xlab = expression(bold("Days")),
        ylim = c(0.94, 1.0),
        xlim = c(0, 60),
        palette = c("#E7B800", "#2E9FDF"),
        title = " Risk of AKI within 60 Days following Exposure to COVID-19 Antigens",
        font.title = c(16, "bold", "Darkblue"),
        censor.shape="|", 
        censor.size = 4,
        ggtheme = theme_light(), 
        size = 1.5,
        risk.table = TRUE,
        pval = TRUE,
        pval.method = TRUE,
        pval.coord = c(0, 0.94),
        legend.title = "",           
        legend.labs = c('Vaccination', 'Infection'),              
        risk.table.height = 0.25, 
        risk.table.y.text = FALSE,
        risk.table.y.text.col = T,
        break.x.by = 10,
        data = SA60TABLE1
        )

    print(ab)

    print(plot)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.81b804cc-5246-423f-8fa5-62fe45f80bf7"),
    SA60TABLE1=Input(rid="ri.foundry.main.dataset.cc92d73d-a942-4f57-bc8a-14ffc8f1c177")
)
SA60_COX_m <- function(SA60TABLE1) {
    library(survival)
    library(survminer)
    library(ggpubr)

    SA60TABLE1$race <- as.factor(SA60TABLE1$race)

    SA60TABLE1$race <- relevel(SA60TABLE1$race, ref = "white")

    SA60TABLE1 <- within(SA60TABLE1, {gender <- factor(gender, labels = c('female', 'male'))})

    SA60TABLE1 <- within(SA60TABLE1, {group_id <- factor(group_id, labels = c('vaccination', 'infection'))})

    AKI.cox <- coxph(Surv(AKI_interval_2, has_AKI) ~ group_id + past_AKI + age_category + gender + race + ethnicity + hypertension + diabetes_mellitus + heart_failure + 
       cardiovascular_disease + obesity, data =  SA60TABLE1)

    a <- summary(AKI.cox)

    print( a )

}

@transform_pandas(
    Output(rid="ri.vector.main.execute.044866d6-e1e6-4733-a713-1dd1f436f62d"),
    SA60TABLE1=Input(rid="ri.foundry.main.dataset.cc92d73d-a942-4f57-bc8a-14ffc8f1c177")
)
SA60_COX_u <- function(SA60TABLE1) {
    library(survival)
    library(survminer)
    library(ggpubr)

    SA60TABLE1 <- within(SA60TABLE1, {group_id <- factor(group_id, labels = c('vaccination', 'infection'))})

    AKI.cox <- coxph(Surv(AKI_interval_2, has_AKI) ~ group_id, data =  SA60TABLE1)

    a <- summary(AKI.cox)

    print( a )

}

@transform_pandas(
    Output(rid="ri.vector.main.execute.85bb74e5-204b-4f89-ae02-bfad9056aa86"),
    SA90TABLE1=Input(rid="ri.foundry.main.dataset.e9e46282-dc3c-44b4-adfd-412004901484")
)
SA90_COX_m <- function(SA90TABLE1) {

    library(survival)

    SA90TABLE1$race <- as.factor(SA90TABLE1$race)

    SA90TABLE1$race <- relevel(SA90TABLE1$race, ref = "white")

    SA90TABLE1 <- within(SA90TABLE1, {gender <- factor(gender, labels = c('female', 'male'))})

    SA90TABLE1 <- within(SA90TABLE1, {group_id <- factor(group_id, labels = c('vaccination', 'infection'))})

    AKI.cox <- coxph(Surv(AKI_interval_2, has_AKI) ~ group_id + past_AKI + age_category + gender + race + ethnicity + hypertension + diabetes_mellitus + heart_failure + 
       cardiovascular_disease + obesity, data = SA90TABLE1)

    a <- summary(AKI.cox)

    print( a )

}

@transform_pandas(
    Output(rid="ri.vector.main.execute.c11a5b8e-54ba-4f8d-ab3d-bd5ca0329eb8"),
    SA90TABLE1=Input(rid="ri.foundry.main.dataset.e9e46282-dc3c-44b4-adfd-412004901484")
)
SA90_COX_u <- function( SA90TABLE1) {
    library(survival)
    library(survminer)
    library(ggpubr)

    SA60TABLE1 <- within(SA90TABLE1, {group_id <- factor(group_id, labels = c('vaccination', 'infection'))})

    AKI.cox <- coxph(Surv(AKI_interval_2, has_AKI) ~ group_id, data =  SA90TABLE1)

    a <- summary(AKI.cox)

    print( a )

}

@transform_pandas(
    Output(rid="ri.vector.main.execute.fd770f4a-3660-4284-8381-32f5d463fa04"),
    SA90TABLE1=Input(rid="ri.foundry.main.dataset.e9e46282-dc3c-44b4-adfd-412004901484")
)
 SA90_km <- function(SA90TABLE1) {
    
    library(survival)
    library(survminer)

    KM <- survfit(Surv(AKI_interval_2, has_AKI ) ~  group_id, data = SA90TABLE1)    #  AKI_by_code as outcome

    plot <- ggsurvplot(
        KM,
        ylab = expression(bold("Probability of NOT developing AKI")),
        xlab = expression(bold("Days")),
        ylim = c(0.94, 1.0),
        xlim = c(0, 90),
        lwd = 8,
        risk.table = TRUE,
        pval = TRUE,
        pval.method = TRUE,
        pval.coord = c(1, 1),
        data = SA90TABLE1
        )

    print(plot)
    
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.3dc3deb0-2219-469d-8057-d320d68ffe6e"),
    SA90TABLE1=Input(rid="ri.foundry.main.dataset.e9e46282-dc3c-44b4-adfd-412004901484")
)
 SA90_km_1 <- function(SA90TABLE1) {
    
    library(survival)
    library(survminer)

    KM <- survfit(Surv(AKI_interval_2, has_AKI ) ~  group_id, data = SA90TABLE1)    #  AKI_by_code as outcome

    plot <- ggsurvplot(
        KM,
        conf.int = TRUE,  
        ylab = expression(bold("Probability of NOT developing AKI")),
        xlab = expression(bold("Days")),
        ylim = c(0.94, 1.0),
        xlim = c(0, 90),
        palette = c("#E7B800", "#2E9FDF"),
        title = " Risk of AKI within 90 Days following Exposure to COVID-19 Antigens",
        font.title = c(16, "bold", "Darkblue"),
        censor.shape="|", 
        censor.size = 4,
        ggtheme = theme_light(), 
        size = 1.5,
        risk.table = TRUE,
        pval = TRUE,
        pval.method = TRUE,
        pval.coord = c(0, 0.94),
        legend.title = "",           
        legend.labs = c('Vaccination', 'Infection'),              
        risk.table.height = 0.25, 
        risk.table.y.text = FALSE,
        risk.table.y.text.col = T,
        break.x.by = 10,
        data = SA90TABLE1
        )

    print(plot)
    
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.1268624d-2d71-4f92-b229-4cefe8641803"),
    HR_plot=Input(rid="ri.vector.main.execute.c22347d6-8b87-462b-b7cc-36a272cc7fd3")
)
plot <- function(HR_plot) {

    HR_plot$Column2 <- format(HR_plot$Column2, big.mark = ",", scientific = FALSE)
    HR_plot$Column3 <- format(HR_plot$Column3, big.mark = ",", scientific = FALSE)
    HR_plot$Column4 <- format(HR_plot$Column4, big.mark = ",", scientific = FALSE)
    HR_plot$Column5 <- format(HR_plot$Column5, big.mark = ",", scientific = FALSE)

    tabletext <- cbind((c("Outcomes","\n",'30-Day Observational Period', "Defined by diagnostic Code", "Defined by Serum Creatinine Change", "Defined by both method", "60-Day Observational Period", "90-Day Observational Period", "Phase 1:Alpha", "Phase 2:Delta", "Phase 3: Omicron")),
    (c("Vaccination Group\nNo. of AKI patients(N)","\n", HR_plot$Column2)),
    (c("Vaccination Group\nNo of NOT AKI patients(N)","\n", HR_plot$Column3)),
    (c("Infection Group\nNo. of AKI patients(N)","\n", HR_plot$Column4 )),
    (c("Infection Group\nNo. of NOT AKI patients(N)","\n", HR_plot$Column5))
     )
    
    plot <- forestplot(
    labeltext=tabletext,
    mean =c(NA,NA,HR_plot$Column6), 
    lower =c(NA,NA,HR_plot$Column7), 
    upper =c(NA,NA,HR_plot$Column8),
    graph.pos=2,
    title = "Harzad ratio of different outcomes",
    xlab = "HR",
    xticks = c( 4, 7, 10,13,17 ),
    new_page = TRUE,
    zero = 3,
    boxsize = 0.15,
    graphwidth = unit(.2, "npc"), 
    txt_gp = fpTxtGp(                # 文本样式的设置
    label = gpar(cex = 0.7),       # 标签的大小
    ticks = gpar(cex = 0.7),         # 刻度标记的大小
    xlab = gpar(cex = 0.7),        # x轴标签的大小
    title = gpar(cex = 1.2),       # 标题的大小
    cex = 0.75             #文本缩放比例
     ),
    ci.vertices = TRUE,
    fn.ci_norm = "fpDrawDiamondCI",
    lwd.ci=2,
    box = gpar(lty = 2, col = "lightgray"),
    col=fpColors(box="black", lines="#1c61b6", zero = "gray50"),
    hrzl_lines = list(               # 水平线样式的设置
    "1" = gpar(lty = 1, lwd = 2),  # 均值线
    "2" = gpar(lty = 2),           # 下限和上限之间的虚线
    "12" = gpar(lwd = 2, lty = 1, columns = c(3:6)) # 下限和上限线
         ))
         
   print(plot)
}

