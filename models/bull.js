const _ = require('lodash')
const Queue = require('bull')

const createQueue = _.memoize(
	(queue, port, host, password, opts) => Queue(queue, { redis: { port, host, password, opts } }),
	queue => queue
)

module.exports.createJob = (redisOptions, queueName, payload) => {
	const { host, port, password, options } = redisOptions
	const queue = createQueue(queueName, port, host, password, options || {})
	return queue.add(payload)
}
