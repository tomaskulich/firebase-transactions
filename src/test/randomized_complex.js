import Firebase from 'firebase'
import {Promise} from 'bluebird'
import {fromJS} from 'immutable'
import * as u from '../useful'
import {transactor} from '../transactor'
import {read, set, push} from '../firebase_actions'
import {TODO_TRX_PATH, DONE_TRX_PATH} from '../settings'

const firebaseUrl = 'https://gugugu.firebaseio.com'
const firebase = new Firebase(firebaseUrl)

export function test({trCount, baseCredit, maxTrCredit, userCount, maxWait}) {

  function randomDelay(maxWait) {
    return (_) => Promise.delay(u.randRange(maxWait))
  }

  const keys = ['a', 'b', 'c']
  const weighedDepths = [[0, 0.1], [1, 0.2], [2, 0.2], [3, 1]]
  const depth = weighedDepths.length - 1

  const pay = ({read, set, push, abort}, data) => {

    function randomSplit(path) {
      let i = u.randomChoiceWeighed(weighedDepths)
      return [path.slice(0, i), path.slice(i)]
    }

    function validate(user, path) {
      return read(['user', user, 'credit', ...path])
      .then((v) => {
        if (v < 0) {
          abort('not enough funds')
        }
      })
    }

    function randomRead(user, path) {
      let [p1, p2] = randomSplit(path)
      let p1path = ['user', user, 'credit', ...p1]
      return read(p1path)
      .then((r) => {
        if (p2.length === 0) {
          return r
        } else {
          return fromJS(r).getIn(p2)
        }
      })
    }

    function randomUpdateBy(user, path, val) {
      let [p1, p2] = randomSplit(path)
      let p1path = ['user', user, 'credit', ...p1]
      return read(p1path)
      .then((r) => {
        if (p2.length === 0) {
          return set(p1path, r + val)
        } else {
          return set(p1path, fromJS(r).updateIn(p2, (v) => v + val).toJS())
        }
      })
    }

    let wait = Math.round(Math.random() * maxWait)
    let {userFrom, userTo, pathFrom, pathTo, credit} = data
    let whenToValidate = u.randomChoice([0, 1])
    let checkRead = u.randomChoice([true, false])
    let beforeCredit
    return randomDelay(wait)()
    .then((_) => !checkRead && randomRead(userFrom, pathFrom)
      .then((_beforeCredit) => beforeCredit = _beforeCredit))
    .then(randomDelay(wait))
    .then((_) => randomUpdateBy(userFrom, pathFrom, -credit))
    .then((_) => !checkRead && randomRead(userFrom, pathFrom)
      .then((afterCredit) => {
        if (beforeCredit - credit !== afterCredit) throw new Error('read is invalid')
      }))
    .then(randomDelay(wait))
    .then((_) => (whenToValidate === 0) && validate(userFrom, pathFrom))
    .then(randomDelay(wait))
    .then((_) => randomUpdateBy(userTo, pathTo, credit))
    .then(randomDelay(wait))
    .then((_) => (whenToValidate === 1) && validate(userFrom, pathFrom))
    .then(randomDelay(wait))
  }

  const handlers = {
    pay,
  }

  function getRandomPath() {
    let res = []
    u.repeat(depth, (_) => res.push(u.randomChoice(keys)))
    return res
  }

  function flattenCredit(creditObj) {
    if (typeof creditObj === 'number') {
      return creditObj
    } else {
      creditObj = fromJS(creditObj)
      let res = fromJS([])
      creditObj.forEach((val, key) => {
        res = res.push(flattenCredit(val))
      })
      return res.flatten()
    }
  }

  function getRandomUser() {

    function getCredit(depth) {
      if (depth === 0) {
        return baseCredit
      } else {
        let res = {}
        for (let key of keys) {
          res[key] = getCredit(depth - 1)
        }
        return res
      }
    }

    return {
      name: 'johny_' + Math.random().toString(36).substring(7),
      credit: getCredit(depth)
    }

  }

  function getRandomTransaction(usersIds) {
    const userFrom = u.randomChoice(usersIds)
    let userTo
    while (true) {
      userTo = u.randomChoice(usersIds)
      if (userTo !== userFrom) {
        break
      }
    }
    let pathFrom = getRandomPath()
    let pathTo = getRandomPath()
    const credit = u.randRange(maxTrCredit)
    return {type: 'pay', data: {userFrom, userTo, credit, pathFrom, pathTo}}
  }

  function run() {
    let toWait = []
    toWait.push(set(firebase, null))
    toWait.push(set(firebase.child('__internal/fbkeep'), 'fbkeep'))

    const userRef = firebase.child('user')
    const usersIds = []
    u.repeat(userCount, (i) => {
      usersIds.push(i)
      let user = getRandomUser()
      user.id = i
      toWait.push(set(userRef.child(i), user))
    })

    const transactionRef = firebase.child(TODO_TRX_PATH)
    for (let i = 0; i < trCount; i++) {
      toWait.push(push(transactionRef, getRandomTransaction(usersIds)))
    }

    let handler

    return Promise.all(toWait).then((_) => {
      let processedCount = 0
      // start transactor; process all submitted transactions
      handler = transactor(firebase, handlers)
      // completes, when we have ${trCount} closed transactions
      return new Promise((resolve, reject) => {
        firebase.child(DONE_TRX_PATH).on('child_added', () => {
          processedCount += 1
          if (processedCount === trCount) {
            resolve()
          }
        })
      })
    })
    .then((_) => read(firebase.child('user')))
    .then((users) => {
      handler.stop()
      users = fromJS(users)
      let creditSeq = users.valueSeq().flatMap((user) => flattenCredit(user.get('credit')))
      let sumCredit = u.sum(creditSeq)
      let minCredit = Math.min.apply(null, creditSeq.toJS())
      return {sumCredit, minCredit, trSummary: handler.trSummary}
    })
  }

  return run()
}
