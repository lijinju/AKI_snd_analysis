

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

