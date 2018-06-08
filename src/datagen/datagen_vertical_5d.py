
import random

datafile = open('../../data/verticalData_5d.csv', 'w')

print("BDATE,DEAL_ID,PROD_ID,PORTFOLIO,SCENARIO,PNLS")

num_of_trades = 1000
num_of_scenarios = 100

bdate="20170228"
product_ids = ["PROD_001", "PROD_002", "PROD_003","PROD_004","PROD_005","PROD_006","PROD_007","PROD_008","PROD_009","PROD_010","PROD_011","PROD_012","PROD_013","PROD_014","PROD_015",]
portfolios = ["PORTF_001", "PORTF_002", "PORTF_003","PORTF_004","PORTF_005","PORTF_006","PORTF_007","PORTF_008","PORTF_009","PORTF_010"]

dimOne = ["dmOne_001", "dmOne_002", "dmOne_003","dmOne_004","dmOne_005","dmOne_006","dmOne_007","dmOne_008"]
dimTwo = ["dmTwo_001", "dmTwo_002", "dmTwo_003","dmTwo_004","dmTwo_005","dmTwo_006"]
dimThree = ["dmThree_001", "dmThree_002", "dmThree_003","dmThree_004","dmThree_005","dmThree_006","dmThree_007","dmThree_008","dmThree_009","dmThree_010"]

random.seed(200)

for line_num in range(1, num_of_trades+1):
    deal_id = "DEAL_" + str(line_num).rjust(8,'0')

    prod_index = random.randrange(0,15,1)
    product_id = product_ids[prod_index]

    portfolio_index = random.randrange(0,10,1)
    portfolio_id = portfolios[portfolio_index]

    dimOne_index = random.randrange(0,8,1)
    dimOne_id = dimOne[dimOne_index]

    dimTwo_index = random.randrange(0,6,1)
    dimTwo_id = dimTwo[dimTwo_index]

    dimThree_index = random.randrange(0,10,1)
    dimThree_id = dimThree[dimThree_index]

    pnl_str = ""

    # dealMu and dealSigma are meant to be distribution parameters for a deal.
    dealMu = random.uniform(-1,1)
    dealSigma = random.uniform(0.05,0.2)
    scenarioId = 0
    for xx in range(0, num_of_scenarios):
        pnl = format(random.normalvariate(dealMu, dealSigma) * 50, '.2f')
        dataline = bdate + "," + deal_id + "," + product_id + "," + portfolio_id + "," \
		   + dimOne_id + "," + dimTwo_id + "," + dimThree_id + "," \
                   + str(scenarioId) + "," + str(pnl) + "\n"
        #print(dataline)
        datafile.write(dataline)

        scenarioId = scenarioId+1


datafile.close()


