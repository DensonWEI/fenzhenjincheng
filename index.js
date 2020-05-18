/*
 * Created by dashan~changjiang on 2020/4/15 16:04.
 */
'use strict'

const {groupId,topic} = require("./config")
const { Kafka, logLevel } = require('kafkajs')
const address = require('address')
const ip = address.ip()
const host = process.env.HOST_IP || address.ip()
const pid = process.pid

console.log(`程序启动信息为：${host} ${pid}`);

const kafka = new Kafka({
	// logLevel: logLevel.ERROR,
	brokers: ['192.168.1.130:9092'],
	clientId: 'my-'+ip+pid,
})
const consumer = kafka.consumer( { groupId:groupId } )


const main = async () => {
	
	var allnum = 0
	await consumer.connect()
	await consumer.subscribe({ topic:topic })
	await consumer.run({
		
		autoCommitThreshold: 1,
		// eachBatch: async ({ batch }) => {
		//   console.log( batch )
		// },
		
		eachMessage: async ( { topic, partition, message } ) => {
			const value = message.value.toString()
			const offset = message.offset
			
			console.log( offset +value );
			
			allnum++
			await sleep(500)
		}
	})
	console.log(allnum);
}


main().catch(e => console.error(`[example/consumer] ${e.message}`, e))



const sleep = (timeountMS) => new Promise((resolve) => {
	setTimeout(resolve, timeountMS);
});




const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']




errorTypes.map(type => {
	process.on(type, async e => {
		try {
			console.log(`process.on ${type}`)
			console.error(e)
			await consumer.disconnect()
			process.exit(0)
		} catch (_) {
			process.exit(1)
		}
	})
})






signalTraps.map(type => {
	process.once(type, async () => {
		try {
			await consumer.disconnect()
		} finally {
			process.kill(process.pid, type)
		}
	})
})