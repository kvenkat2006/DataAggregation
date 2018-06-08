
import random

datafile = open('../../data/verticalData_2d.csv', 'w')
transformedDatafile = open('../../data/transformedVerticalData_2d.csv', 'w')

print("BDATE,DEAL_ID,PROD_ID,PORTFOLIO,SCENARIO,PNLS")

num_of_trades = 1000
num_of_scenarios = 100

bdate="20170228"
transformedBDate = "02/28/2017"
product_ids = ["PROD_001", "PROD_002", "PROD_003","PROD_004","PROD_005","PROD_006","PROD_007","PROD_008","PROD_009","PROD_010","PROD_011","PROD_012","PROD_013","PROD_014","PROD_015",]
portfolios = ["PORTF_001", "PORTF_002", "PORTF_003","PORTF_004","PORTF_005","PORTF_006","PORTF_007","PORTF_008","PORTF_009","PORTF_010"]

prodDescFile = open('../../data/product_descriptions.csv', 'w')

for idx in range(0,len(product_ids)):
    prodDescFile.write( product_ids[idx] + ", Description of " + product_ids[idx] + "\n")

random.seed(200)

for line_num in range(1, num_of_trades+1):
    deal_id = "DEAL_" + str(line_num).rjust(8,'0')

    prod_index = random.randrange(0,15,1)
    product_id = product_ids[prod_index]

    portfolio_index = random.randrange(0,10,1)
    portfolio_id = portfolios[portfolio_index]

    pnl_str = ""
    sss = 0  ## This variable is to select a negative or a positive number at random.

    # dealMu and dealSigma are meant to be distribution parameters for a deal.
    dealMu = random.uniform(-1,1)
    dealSigma = random.uniform(0.05,0.2)
    scenarioId = 0
    for xx in range(0, num_of_scenarios):
        pnl = format(random.normalvariate(dealMu, dealSigma) * 50, '.2f')
        dataline = bdate + "," + deal_id + "," + product_id + "," + portfolio_id + "," \
                   + str(scenarioId) + "," + str(pnl) + "\n"
        #print(dataline)
        datafile.write(dataline)

        transformedDataLine = transformedBDate + "," + deal_id + "," + product_id + ",Description of " + product_id + "," + portfolio_id + "," \
                   + str(scenarioId) + "," + str(pnl) + "\n"
        transformedDatafile.write(transformedDataLine)
        scenarioId = scenarioId+1


datafile.close()
transformedDatafile.close()


