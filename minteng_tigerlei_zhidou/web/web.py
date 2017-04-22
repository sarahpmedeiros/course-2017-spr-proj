from flask import Flask, render_template, request
import optimization_algorithm

app = Flask(__name__)

info=None


@app.route("/")
def index():
    return render_template('index.html')

@app.route("/optimize/", methods=["GET"])
def optimize():
    return render_template('optimize.html')

@app.route("/optimize-result/", methods=["GET"])
def optimize_result():

    food=request.args["Food"]
    transport=request.args['Transport']
    safe=request.args['Safety']
    rent=request.args['Rent']
    # print(food,transport,safe,rent)
    result=optimization_algorithm.get_result(int(food),int(transport),int(safe),int(rent))
    res=result[:5]
    for i in res:
        i['grade']=[i['grade']['food'],i['grade']['transport'],i['grade']['safety'],i['grade']['rent']]
    global info
    info=res
    # print(res)
    return render_template('optimize-result.html',f=food,t=transport,s=safe,r=rent,res=res)

@app.route("/map/")
def get_map():
    global info
    return render_template('map.html',a=info[0],b=info[1],c=info[2],d=info[3],e=info[4])

@app.route("/static-analysis/")
def static_analysis():
    return render_template('static-analysis.html')


if __name__ == '__main__':
    app.run(debug=True)