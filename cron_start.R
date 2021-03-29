library(cronR)
monitor_script <- "/monitor.R"
cmd <- cron_rscript(monitor_script)
print(cmd)
cron_add(command = cmd, frequency = '4-59/6 * * * *', id = 'process_data', description = 'Process data by compensating for atmospheric pressure')