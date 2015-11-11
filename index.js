'use strict';

var express = require('express');
var http = require('http');
var request = require('request');
var qs = require('querystring');
var md5 = require('md5');
var fs = require('fs');

require('dotenv').config({silent: true});

var mix_secret = process.env.MIX_SECRET,
mix_key = process.env.MIX_KEY,
PORT = process.env.PORT || 8080;


var app = express();

function dateFormat(date){
    return date.toISOString().split('T')[0]
}

function getQueryParams(params){
    params.api_key = mix_key;
    var keys = Object.keys(params).sort(),
    sig_string = '',
    query_string = '';
    keys.forEach(function(key) {
        var param_string = key + '=' + params[key]
        sig_string += param_string;
        query_string += param_string + '&';
    });
    var sig = md5(sig_string+mix_secret);
    return encodeURI(query_string+'sig='+sig);
}

function full_request(){
    return new Promise(function(total_resolve, total_reject){

        var engage_params = getQueryParams({
            expire: Date.now()+2000
        });

        request('https://mixpanel.com/api/2.0/engage?'+engage_params, function(error, response, body){
            console.log("got ids");
            if(!error){
                var ids = JSON.parse(body).results.map(function(user){
                    return user.$distinct_id.toString();
                }), data = [];
                var promises = ids.map(function(id){
                    var stream_params = getQueryParams({
                        to_date: dateFormat( new Date() ),
                        from_date: dateFormat( new Date(Date.now()-604800000) ),
                        expire: Date.now()+2000,
                        distinct_ids: '["'+id+'"]'
                    });
                    return new Promise(function(resolve, reject){
                        request('https://mixpanel.com/api/2.0/stream/query?'+stream_params, function (error, response, body) {
                            if(!error){
                                var result = JSON.parse(body);
                                data.push({
                                    id: id,
                                    events: result.results.events.map(function(event){
                                        return {
                                            event: event.event,
                                            time: event.properties.time
                                        }
                                    })
                                });
                                resolve();
                            } else {
                                reject(error);
                            }
                        })
                    });
                });
                Promise.all(promises).then(function(){
                    total_resolve(data);
                }, function(error){
                    total_reject(error);
                });
            } else {
                total_reject(error);
            }
        });
    });
}

function toCSV(data){
    var fields = 'id, event, time\n';
    var parsed = data.map(function(item){
        if(item.events.length < 1) return 'none'
        var events = item.events.map(function(event){
            return item.id + ',' + event.event + ',' + event.time;
        });
        return events.join('\n')
    })
    var filtered = parsed.filter(function(item){
        return item != 'none';
    })
    return fields + filtered.join('\n');
}


app.get('/', function (req, res) {

    try {
        full_request().then(function(data){
            console.log("Resolved all");
            res.set({
                'Content-disposition': "attachment;filename=file.csv",
                'Content-Type': 'text/csv'
            })
            res.send(toCSV(data));
        }, function(error){
            console.log(error);
        });
    } catch(ex){
        console.error(ex);
    }


});
//
// fs.readFile('./file.json', function (err, data) {
//   var obj = JSON.parse(data);
//   console.log(toCSV(obj));
// });


var server = app.listen(PORT, function () {
    var host = server.address().address;
    var port = server.address().port;

    console.log('Example app listening at http://%s:%s', host, port);
});
