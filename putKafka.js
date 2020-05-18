/*
 * Created by dashan~changjiang on 2020/4/14 9:50.
 */




const { Kafka , Partitioners } = require('kafkajs')
var { getMetaData, getMetaDuration, takeScreenshots } = require('./ffmpegMethod')




const kafka = new Kafka({
	clientId: 'my-sdfsadfsdfsadfad',
	brokers: ['192.168.1.130:9092']
})


const MyPartitioner = () => {
	return ({ topic, partitionMetadata, message }) => {
		// select a partition based on some logic
		// return the partition number
		
		console.log("@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		console.log( topic );
		console.log( partitionMetadata );
		console.log( message );
		console.log("@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		return message.partition
	}
}


// { createPartitioner: MyPartitioner }

const producer = kafka.producer( { createPartitioner: Partitioners.JavaCompatiblePartitioner } )




async function aaa(){}




(async ()=>{
	
	
	let url = "http://223.223.180.17/video/iqy/%e6%bf%80%e6%83%85%e7%9a%84%e5%b2%81%e6%9c%88/%e6%bf%80%e6%83%85%e7%9a%84%e5%b2%81%e6%9c%8830a.mp4"
	let framesBaseUrl ="./dhg66dgsuweqx/"
	
	
	let metainfo2 = await getMetaData(url)
	
	
	if(!metainfo2){
		console.log("false false false false false!!!!!!!!!!!!");
		return
	}
	
	let durationMS = metainfo2.video.duration * 1000
	
	let baseStepTime = 10000
	let StepTime = baseStepTime - 1
	let beginTime = 1
	
	
	// 视频地址，
	// 总帧数，
	// 当前帧数，
	// 帧名称
	
	
	let MSArray = []
	for( beginTime; beginTime<=durationMS ; ){
		MSArray.push(beginTime)
		beginTime = beginTime + StepTime
	}
	
	let messages = MSArray.map( (item,index)=>{
		
		console.log(index);
		
		let elem = {
			url:url,
			framesNum:MSArray.length,
			framesIndex:index+1,
			framesName:`${framesBaseUrl}${"dhg66dgsuweqx"}_${item}.png`
		}
		
		return { value: JSON.stringify(elem)  }
	})
	
	console.log(messages);
	
	await producer.connect()
	
	
	for( let i=0; i < messages.length; i++){
		console.log(i);
		await producer.send({topic: 'frames', messages: [ messages[i] ] })
	}
	
	
	await producer.disconnect()
		
	
		
		

	
	
	
	
	console.log("end!!!!!!!!!!!!!");
})()



