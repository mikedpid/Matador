const redisModel = require('../models/redis')
const moment = require('moment')
const _ = require('lodash')

module.exports = function (app) {
	const getDelayedModel = function () {
		return redisModel.getStatus('delayed')
			.then(delayed => redisModel.getJobsInList(delayed))
			.then(keys => redisModel.formatKeys(keys))
			.then(formattedKeys => redisModel.getDelayTimeForKeys(formattedKeys))
			.then(keyList => Promise.all([keyList, redisModel.getStatusCounts()]))
			.then(([keyList, countObject]) => {
				keyList = _.map(keyList, key => {
					const secondsUntil = moment(new Date(key.delayUntil)).diff(moment(), 'seconds')
					const formattedDelayUntil = secondsUntil > 60
						? moment(new Date(key.delayUntil)).fromNow()
						: `in ${secondsUntil} seconds`
					key.delayUntil = formattedDelayUntil
					return key
				})

				return {
					keys: keyList,
					counts: countObject,
					delayed: true,
					type: 'Delayed'
				}
			})
	}

	app.get('/delayed', function (req, res) {
		return getDelayedModel()
			.then(model => res.render('jobList', model))
	})

	app.get('/api/delayed', function (req, res) {
		return getDelayedModel()
			.then(model => res.json(model))
	})
}
