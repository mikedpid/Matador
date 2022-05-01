const _ = require('lodash')

const getActiveKeys = function (queueName) {
	return new Promise(resolve => {
		queueName = queueName ? queueName : '*'
		redis.keys(`bull:${queueName}:active`, (err, keys) =>resolve(keys))
	})
}

const getCompletedKeys = function (queueName) {
	return new Promise(resolve => {
		queueName = queueName ? queueName : '*';
		redis.keys(`bull:${queueName}:completed`, (err, keys) => resolve(keys))
	})
}

const getFailedKeys = function (queueName) {
	return new Promise((resolve) => {
		queueName = queueName ? queueName : '*';
		redis.keys(`bull:${queueName}:failed`, (err, keys) => resolve(keys))
	})
}

const getWaitingKeys = function (queueName) {
	return new Promise((resolve) => {
		queueName = queueName ? queueName : '*';
		redis.keys(`bull:${queueName}:wait`, (err, keys) => resolve(keys))
	})
}

const getDelayedKeys = function (queueName) {
	return new Promise((resolve) => {
		queueName = queueName ? queueName : '*';
		redis.keys(`bull:${queueName}:delayed`, (err, keys) => resolve(keys))
	})
}

const getStuckKeys = () => {
	return getAllKeys()
		.then(keys => formatKeys(keys))
		.then(keyList => {
			const filtered = _.filter(keyList, key => key.status === 'stuck')
			const keys = {}
			let count = 0
			for (let i = 0; i < filtered.length; i++) {
				if (!keys[filtered[i].type]) keys[filtered[i].type] = []
				keys[filtered[i].type].push(filtered[i].id)
				count++
			}

			return { keys, count }
		})
}

const getStatus = function (status, queueName) {
	return new Promise((resolve, reject) => {
		let getStatusKeysFunction = null

		if (status === 'complete') {
			getStatusKeysFunction = getCompletedKeys
		} else if (status === 'active') {
			getStatusKeysFunction = getActiveKeys
		} else if (status === 'failed') {
			getStatusKeysFunction = getFailedKeys
		} else if (status === 'wait') {
			getStatusKeysFunction = getWaitingKeys
		} else if (status === 'delayed') {
			getStatusKeysFunction = getDelayedKeys
		} else if (status === 'stuck') {
			return getStuckKeys()
		} else {
			return reject(new Error('UNSUPPORTED STATUS:', status))
		}

		return getStatusKeysFunction(queueName)
			.then(keys => {
				const multi = []
				const statusKeys = []
				
				for (var i = 0, ii = keys.length; i < ii; i++) {
					const arr = keys[i].split(':')
					const queueName = arr.slice(1, arr.length - 1)
					const queue = queueName.join(':')
					statusKeys[queue] = [] // This creates an array/object thing with keys of the job type
					switch (status) {
						case 'active':
						case 'wait':
							multi.push(['lrange', keys[i], 0, -1])
							break
						case 'delayed':
						case 'complete':
						case 'failed':
							multi.push(['zrange', keys[i], 0, 1])
						default:
							multi.push(['smembers', keys[i]])
							break
					}
				}

				redis.multi(multi).exec((err, data) => {
					const statusKeyKeys = Object.keys(statusKeys) // Get the keys from the object we created earlier...
					let count = 0
					for (let k = 0; k < data.length; k++) {
						statusKeys[statusKeyKeys[k]] = data[k]
						count += data[k].length
					}

					return resolve({
						keys: statusKeys,
						count: count
					})
				})
			})
	})
}

const getAllKeys = function () {
	return new Promise((resolve) => {
		redis.keys('bull:*:[0-9]*', (err, keysWithLocks) => {
			const keys = []
			for (let i = 0; i < keysWithLocks.length; i++) {
				const keyWithLock = keysWithLocks[i]
				if (keyWithLock.substring(keyWithLock.length - 5, keyWithLock.length) !== ':lock') {
					keys.push(keyWithLock)
				}
			}
			resolve(keys)
		})
	})
}

const getFullKeyNamesFromIds = function (list) {
	return new Promise((resolve) => {
		if (!list) return resolve()
		if (!(list instanceof Array)) resolve()

		const keys = []
		for (let i = 0; i < list.length; i++) {
			keys.push(['keys', 'bull:*:' + list[i]])
		}

		redis.multi(keys).exec((err, arrayOfArrays) => {
			const results = []
			for (var i = 0; i < arrayOfArrays.length; i++) {
				if (arrayOfArrays[i].length === 1) {
					results.push(arrayOfArrays[i][0])
				}
			}
			resolve(results)
		})
	})
}

const getJobsInList = function (list) {
	return new Promise((resolve) => {
		if (!list) return resolve()
		
		if (list['keys']) {
			//New list type
			const keys = list['keys']
			const objectKeys = Object.keys(keys)
			const fullNames = []
			for (let i = 0; i < objectKeys.length; i++) {
				for (let k = 0; k < keys[objectKeys[i]].length; k++) {
					fullNames.push(`bull:${objectKeys[i]}:${keys[objectKeys[i]][k]}`)
				}
			}
			return resolve(fullNames)
		}

		//Old list type
		getFullKeyNamesFromIds(list).then(keys => resolve(keys))
	})
}

const getStatusCounts = () => {
	return new Promise((resolve, reject) => {
		return Promise.all([
			getStatus('active'),
			getStatus('complete'),
			getStatus('failed'),
			getStatus('wait'),
			getStatus('delayed'),
			getAllKeys()
		])
			.then(([active, completed, failed, pendingKeys, delayedKeys, allKeys]) => {
				redis.keys('bull:*:id', (err, keys) => {
					const countObject = {
						active: active.count,
						complete: completed.count,
						failed: failed.count,
						pending: pendingKeys.count,
						delayed: delayedKeys.count,
						total: allKeys.length,
						stuck: allKeys.length - (active.count + completed.count + failed.count + pendingKeys.count + delayedKeys.count),
						queues: keys.length
					}

					return resolve(countObject)
				})
			})
	})
}

const formatKeys = keys => {
	return new Promise((resolve, reject) => {
		if (!keys) resolve()
		return Promise.all([
			getStatus('failed'),
			getStatus('complete'),
			getStatus('active'),
			getStatus('wait'),
			getStatus('delayed')
		])
			.then(([failedJobs, completedJobs, activeJobs, pendingJobs, delayedJobs]) => {
				const keyList = []
				for (let i = 0; i < keys.length; i++) {
					const arr = keys[i].split('')
					const queueName = arr.slice(1, arr.length - 1)
					const queue = queueName.join(':')
					const explodedKeys = {}
					explodedKeys[0] = arr[0]
					explodedKeys[1] = queue
					explodedKeys[2] = arr[arr.length - 1]
					// idk how to name this
					const fn = collection => {
						if (!collection.keys[explodedKeys[1]]) return false
						if (typeof collection.keys[explodedKeys[1]].indexOf !== 'function') return false
						if (collection.keys[explodedKeys[1]].indexOf(explodedKeys[2]) === -1) return false
						return true
					}
					let status = 'stuck'
					if (fn(activeJobs)) {
						status = 'active'
					} else if (fn(completedJobs)) {
						status = 'complete'
					} else if (fn(failedJobs)) {
						status = 'failed'
					} else if (fn(pendingJobs)) {
						status = 'pending'
					} else if (fn(delayedJobs)) {
						status = 'delayed'
					}

					keyList.push({
						id: explodedKeys[2],
						type: explodedKeys[1],
						status
					})

					keyList = _.sortBy(keyList, key => parseInt(key.id))
					return resolve(keyList)
				}
			})
	})
}

const removeJobs = function (list) {
	if (!list) return
	//Expects {id: 123, type: "video transcoding"}
	const multi = []
	for (let i = 0; i < list.length; i++) {
		const { id, type } = list[i]
		const firstPartOfKey = `bull:${type}:`
		multi.push(['del', `${firstPartOfKey}${id}`])
		multi.push(['lrem', `${firstPartOfKey}active`, 0, id])
		multi.push(['lrem', `${firstPartOfKey}wait`, 0, id])
		multi.push(['zrem', `${firstPartOfKey}completed`, id])
		multi.push(['zrem', `${firstPartOfKey}failed`, id])
		multi.push(['zrem', `${firstPartOfKey}delayed`, id])
	}
	redis.multi(multi).exec()
}

const makePendingByType = function (type) {
	return new Promise((resolve, reject) => {
		type = type.toLowerCase()
		//I could add stuck, but I won't support mass modifying "stuck" jobs because it's very possible for things to be in a "stuck" state temporarily, while transitioning between states
		const validTypes = ['active', 'complete', 'failed', 'wait', 'delayed']

		if (validTypes.indexOf(type) === -1) {
			return resolve({
				success: false,
				message: `Invalid type: ${type} not in list of supported types`
			})
		}

		return getStatus(type)
			.then(allKeys => {
				const multi = []
				const allKeyObjects = Object.keys(allKeys.keys)
				for (var i = 0; i < allKeyObjects.length; i++) {
					const firstPartOfKey = `bull:${allKeyObjects[i]}:`
					for (let k = 0; k < allKeys.keys[allKeyObjects[i]].length; k++) {
						const item = allKeys.keys[allKeyObjects[i]][k]
						//Brute force remove from everything
						multi.push(['lrem', `${firstPartOfKey}active`, 0, item])
						multi.push(['zrem', `${firstPartOfKey}completed`, item])
						multi.push(['zrem', `${firstPartOfKey}failed`, item])
						multi.push(['zrem', `${firstPartOfKey}delayed`, item])
						//Add to pending
						multi.push(['rpush', `${firstPartOfKey}wait`, item])
					}
				}

				redis.multi(multi).exec((err, data) => {
					if (err) return resolve({ success: false, message: err })
					return resolve({
						success: true,
						message: `Successfully made all ${type} jobs pending.`
					})
				})
		})
	})
}

const makePendingById = function (type, id) {
	return new Promise((resolve, reject) => {
		if (!id) return resolve({ success: false, message: `There was no ID provided.` })
		if (!type) return resolve({ success: false, message: `There was no type provided.` })

		const firstPartOfKey = `bull:${type}:`
		const multi = []
		multi.push([`lrem`, `${firstPartOfKey}active`, 0, id])
		multi.push([`lrem`, `${firstPartOfKey}wait`, 0, id])
		multi.push([`zrem`, `${firstPartOfKey}completed`, id])
		multi.push([`zrem`, `${firstPartOfKey}failed`, id])
		multi.push([`zrem`, `${firstPartOfKey}delayed`, id])
		//Add to pending
		multi.push(['rpush', `${firstPartOfKey}wait`, id])
		redis.multi(multi).exec(function (err, data) {
			if (err) return resolve({ success: false, message: err })
			return resolve({
				success: true,
				message: `Successfully made ${type} job #${id} pending.`
			})
		})
	})
}

const deleteJobByStatus = function (type, queueName) {
	return new Promise((resolve, reject) => {
		type = type.toLowerCase()
		//I could add stuck, but I won't support mass modifying "stuck" jobs because it's very possible for things to be in a "stuck" state temporarily, while transitioning between states
		const validTypes = ['active', 'complete', 'failed', 'wait', 'delayed']

		if (validTypes.indexOf(type) === -1) {
			return resolve({ success: false, message: `Invalid type: ${type} not in list of supported types` })
		}
	
		return getStatus(type, queueName)
			.then(allKeys => {
				const multi = []
				const allKeyObjects = Object.keys(allKeys.keys)
				for (let i = 0; i < allKeyObjects.length; i++) {
					const firstPartOfKey = `bull:${allKeyObjects[i]}:`
					for (let k = 0; k < allKeys.keys[allKeyObjects[i]].length; k++) {
						const item = allKeys.keys[allKeyObjects[i]][k]
						//Brute force remove from everything
						multi.push(["lrem", firstPartOfKey + "active", 0, item]);
						multi.push(["lrem", firstPartOfKey + "wait", 0, item]);
						multi.push(["zrem", firstPartOfKey + "completed", item]);
						multi.push(["zrem", firstPartOfKey + "failed", item]);
						multi.push(["zrem", firstPartOfKey + "delayed", item]);
						multi.push(["del", firstPartOfKey + item]);
					}
				}

				redis.multi(multi).exec((err, data) => {
					if (err) return resolve({ success: false, message: err })

					if (queueName) {
						return resolve({
							success: true,
							message: `Successfully deleted all jobs of status ${type} of queue ${queueName}.`
						})
					}

					return resolve({
						success: true,
						message: `Successfully deleted all jobs of status ${type}.`
					})
				})
			})
	})
}

const deleteJobById = function (type, id) {
	return new Promise((resolve, reject) => {
		if (!id) return resolve({ success: false, message: `There was no ID provided.` })
		if (!type) return resolve({ success: false, message: `There was no type provided.` })
		const firstPartOfKey = `bull:${type}:`
		const multi = []
		multi.push(['lrem', `${firstPartOfKey}active`, 0, id])
		multi.push(['lrem', `${firstPartOfKey}wait`, 0, id])
		multi.push(['zrem', `${firstPartOfKey}completed`, id])
		multi.push(['zrem', `${firstPartOfKey}failed`, id])
		multi.push(['zrem', `${firstPartOfKey}delayed`, id])
		multi.push(['del', `${firstPartOfKey}${id}`])

		redis.multi(multi).exec((err, data) => {
			if (err) return resolve({ success: false, message: err })
			return resolve({ success: true, message: `Successfully deleted job ${type} #${id}.` })
		})
	})
}

const getDataById = function (type, id) {
	return new Promise((resolve, reject) => {
		if (!id) return resolve({ success: false, message: `There was no ID provided.` })
		if (!type) return resolve({ success: false, message: `There was no type provided.` })
		const firstPartOfKey = `bull:${type}:`

		redis.hgetall(firstPartOfKey + id, function (err, data) {
			if (err) return resolve({ success: false, message: err })
			return resolve({ success: true, message: data })
		})
	})
}

const getProgressForKeys = function (keys) {
	return new Promise((resolve, reject) => {
		const multi = []
		for (let i = 0; i < keys.length; i++) {
			multi.push(['hget', `bull:${keys[i].type}:${keys[i].id}`, `progress`])
		}

		redis.multi(multi).exec((err, results) => {
			for (var i = 0; i < keys.length; i++) {
				keys[i].progress = results[i];
			}
			resolve(keys)
		})
	})
}

const getDelayTimeForKeys = function (keys) {
	return new Promise((resolve, reject) => {
		const multi = []
		for (let i = 0; i < keys.length; i++) {
			multi.push(['zscore', `bull:${keys[i].type}:delayed`, keys[i].id])
		}

		redis.multi(multi).exec((err, results) => {
			for (var i = 0; i < keys.length; i++) {
				// Bull packs delay expire timestamp and job id into a single number. This is mostly
				// needed to preserve execution order – first part of the resulting number contains
				// the timestamp and the end contains the incrementing job id. We don't care about
				// the id, so we can just remove this part from the value.
				// https://github.com/OptimalBits/bull/blob/e38b2d70de1892a2c7f45a1fed243e76fd91cfd2/lib/scripts.js#L90
				keys[i].delayUntil = new Date(Math.floor(results[i] / 0x1000))
			}
			return resolve(keys)
		})
	})
}

const getQueues = () => {
	return new Promise((resolve, reject) => {
		redis.keys('bull:*:id').then(queues => {
			return Promise.all(queues.map(async (queue) => {
				let name = queue.substring(0, queue.length - 3);
				let activeJobs = await redis.lrange(name + ":active", 0, -1);
				let active = activeJobs.filter(job => {
					return redis.get(`${name}:${job}:lock`).then(lock => lock != null)
				})
				let stalled = activeJobs.filter(job => {
					return redis.get(`${name}:${job}:lock`).then(lock => lock == null)
				})

				let pending = await redis.llen(`${name}:wait`)
				let delayed = await redis.zcard(`${name}:delayed`)
				let completed = await redis.zcount(`${name}:completed`, '-inf', '+inf')
				let failed = await redis.zcount(`${name}:failed`, '-inf', '+inf')

				return {
					name: name.substring(5),
					active: active.length,
					stalled: stalled.length,
					pending,
					delayed,
					completed,
					failed,
				}
			}))
		}).then(resolve)
	})
}

module.exports.getAllKeys = getAllKeys //Returns all JOB keys in string form (ex: bull:video transcoding:101)
module.exports.formatKeys = formatKeys //Returns all keys in object form, with status applied to object. Ex: {id: 101, type: "video transcoding", status: "pending"}
module.exports.getStatus = getStatus //Returns indexes of completed jobs
module.exports.getStatusCounts = getStatusCounts //Returns counts for different statuses
module.exports.getJobsInList = getJobsInList //Returns the job data from a list of job ids
module.exports.getDataById = getDataById //Returns the job's data based on type and ID
module.exports.removeJobs = removeJobs //Removes one or  more jobs by ID, also removes the job from any state list it's in
module.exports.makePendingByType = makePendingByType //Makes all jobs in a specific status pending
module.exports.makePendingById = makePendingById //Makes a job with a specific ID pending, requires the type of job as the first parameter and ID as second.
module.exports.deleteJobByStatus = deleteJobByStatus //Deletes all jobs in a specific status
module.exports.deleteJobById = deleteJobById //Deletes a job by ID. Requires type as the first parameter and ID as the second.
module.exports.getProgressForKeys = getProgressForKeys //Gets the progress for the keys passed in
module.exports.getDelayTimeForKeys = getDelayTimeForKeys // Gets the delay end time for the keys passed in
module.exports.getQueues = getQueues //Get information about all the queues in the redis instance