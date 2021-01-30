const express=require('express');
const app=express();
const amqplib=require('amqplib/callback_api');

const PORT = 3000||process.env.PORT;

let channel=null;
app.use(express.json())

function InitQueuConnection(callback)
{
    amqplib.connect('amqp://localhost', function(error0, connection) {
    if(error0)
        return console.error(error0);
    console.info('RabbitMQ: Connection established');
    connection.createChannel((error1,channel)=>{
        if(error1)
            return console.error(error1);
        console.info('RabbitMQ: Channel Created');
        //For Reciever 1
        channel.assertQueue('',{exclusive:true},(error3,q)=>{
            if(error3)
               return console.error(error3)
            console.info(`Waiting for messaged in Que [${q.queue}]`)
            channel.bindQueue(q.queue,'user','user.event.create');
            channel.consume(q.queue,(msg)=>{
                console.info(`Reciever 1: ${msg.content.toString()}`);
                channel.ack(msg);
            })
            
        })
         //For Reciever 2
        channel.assertQueue('',{exclusive:true},(error3,q)=>{
            if(error3)
               return console.error(error3)
            console.info(`Waiting for messaged in Que [${q.queue}]`)
            channel.bindQueue(q.queue,'user','user.event.update');
            channel.consume(q.queue,(msg)=>{
                console.info(`Reciever 2: ${msg.content.toString()}`);
            })
            
        })
         //For Reciever 3
        channel.assertQueue('',{exclusive:true},(error3,q)=>{
            if(error3)
               return console.error(error3)
            console.info(`Waiting for messaged in Que [${q.queue}]`)
            channel.bindQueue(q.queue,'user','user.event.delete');
            channel.consume(q.queue,(msg)=>{
                console.info(`Reciever 3: ${msg.content.toString()}`);
            })
            
        })

        //For Publisher
        channel.assertExchange('user','topic',{durable:false});
        callback(channel);


    })
});

}

app.post('/send',(req,res)=>{
    channel.publish('user',req.body.route,Buffer.from(req.body.msg))
    res.send('published msg:'+req.body.msg)
})

InitQueuConnection((ch)=>{
    channel=ch;
    app.listen(PORT,()=>console.info("Application has Started on localhost:"+PORT))
})


//Registering Publisher and Consumer in RabbitMQ

