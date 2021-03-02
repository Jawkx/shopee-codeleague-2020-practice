import pandas as pd
from datetime import datetime

df = pd.read_csv('D:\Projects\Working on\shopee-pyq\orderBrushing\order_brush_order.csv')

def toTimeStamp(timestring):
    return int(datetime.timestamp(datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S')))

df['event_time'] = df['event_time'].apply( lambda x:toTimeStamp(x))
# df.drop(df.tail(219900).index,inplace=True)


# print(df)
class Order:
    def __init__ (self, id , user , time):
        self.id = id
        self.user = user
        self.timestamp = time

class Shop:
    def __init__ (self, id):
        self.id = id
        self.isBrushing = False
        self.orders = []
        self.categorizedOrder = []
        self.brushingUser = {}

    def addOrder(self,order):
        self.orders.append(order)

    def categorizeOrder(self):
        if len(self.orders) < 3:
            return False
            
        self.orders.sort( key = lambda order: order.timestamp)

        startIdx = 0
        while startIdx < len(self.orders):
            temp = []
            startTime = self.orders[startIdx].timestamp
            endTime = startTime + 3600

            endIdx = startIdx
            while endIdx < len(self.orders):
                if (self.orders[endIdx].timestamp <= endTime):
                    temp.append( self.orders[endIdx] )
                    endIdx += 1
                else:
                    break
            
            if len(temp) >= 3 :
                self.categorizedOrder.append( temp )
            
            startIdx = endIdx

    def getBrushingUserStr(self):
        if len(self.brushingUser) == 1:
            return str(list(self.brushingUser.keys())[0])
        else:
            userMostToLess = sorted(self.brushingUser.keys(), key=lambda user: self.brushingUser[user], reverse=True)
            allBrushingUsers = [str(userMostToLess[0])]
            highestScore =  self.brushingUser[userMostToLess[0]]
            userMostToLess.pop(0)

            for user in userMostToLess:
                if self.brushingUser[user] == highestScore:
                    allBrushingUsers.append(str(user))
                else:
                    break
            
            if len(allBrushingUsers) == 1:
                return allBrushingUsers[0]
            else:
                return '&'.join(allBrushingUsers)
            

        return userMostToLess

    def logUser(self):
        if len(self.categorizedOrder) > 0:
            
            for sameTimeOrders in self.categorizedOrder:
                userAppearDict = {}

                for order in sameTimeOrders:
                    currUser = order.user
                    if currUser not in userAppearDict.keys():
                        userAppearDict[currUser] = 1
                    else:
                        userAppearDict[currUser] += 1
                
                if (len(sameTimeOrders) / len(userAppearDict) >=3):
                    self.isBrushing = True

                    userList = list(map(lambda order:order.user , sameTimeOrders))

                    for user in userList:
                        if user not in self.brushingUser:
                            self.brushingUser[user] = 1
                        else:
                            self.brushingUser[user] += 1

                                  
shopDict = {}

for row in df.itertuples(name=None):
    (idx, oID , sID , uID , ts) = row

    if sID not in shopDict.keys():
        newShop = Shop(sID)
        newShop.addOrder( Order(oID , uID , ts))
        shopDict[sID] = newShop
    else:
        shopDict[sID].addOrder( Order(oID , uID , ts) ) 
    
    print(idx)

shopArr = []

for key in shopDict:
    shopArr.append(shopDict[key])

#print(shopArr)

outdf = pd.DataFrame(columns = ["shopid","userid"])

rowindex = 0
for shop in shopArr:
    shop.categorizeOrder()
    shop.logUser()
    if (shop.isBrushing):
        outdf.loc[rowindex] = [ str(shop.id) , shop.getBrushingUserStr() ]
    else:
        outdf.loc[rowindex] = [ str(shop.id) , str(0) ]
    
    rowindex += 1

outdf.to_csv("out.csv",index=False)