const updateInfo = require('../lib/updateInfo.js')
const redisModel = require('../models/redis')
const _ = require('lodash')

module.exports = function (app) {
	const getOverviewData = function () {
		return redisModel.getAllKeys()
			.then(keys => {
				return Promise.all([
					redisModel.formatKeys(keys),
					redisModel.getStatusCounts(),
					updateInfo.getMemoryUsage()
				])
			})
			.then(([keyList, countObject, memoryUsage]) => {
				const usage = []
				keyList = _.filter(keyList, key => key.status === 'stuck')
				for (let time in memoryUsage.usage) {
					usage.push({ time, memory: memoryUsage[time] })
				}
				memoryUsage.usage = usage
				return {
					keys: keyList,
					counts: countObject,
					overview: true,
					memory: memoryUsage
				}
			})
	}

	app.get('/', function (req, res) {
		return getOverviewData()
			.then(model => res.render('index', model))
	})

	app.get('/api/', function (req, res) {
		return getOverviewData()
			.then(model => res.json(model))
	})
}
