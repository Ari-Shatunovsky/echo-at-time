import express from 'express';
import Redis from 'ioredis';
import { of, from } from 'rxjs';
import { expand } from 'rxjs/operators';
import bodyParser from 'body-parser';
import { Message } from './models/message';
import { convertMessageDTO, messageRedisID } from './utils/converters';
import { RedisAccessor } from './accessors/redis-accessor';
import { VALIDATION_ERROR } from './models/consts';

const app: express.Application = express();
const redisClient = new Redis(6379, '127.0.0.1');
const redisAccessor = new RedisAccessor(redisClient);
const messages: Message[] = [];
const timers: Map<string, NodeJS.Timeout> = new Map();
let lastId: number;
let isPastMessagesLoaded = false;

app.use(bodyParser.json());
app.get('/', (req, res) => {
    res.send('Echo at time');
});

app.post('/echoAtTime/', async (req, res) => {
    try {
        const message = convertMessageDTO(req.body);
        await redisAccessor.addMessageToEventStream(message);
        res.sendStatus(200);
    } catch (e) {
        if (e.name && e.name === VALIDATION_ERROR) {
            res.status(400).send(e.message);
        } else {
            res.sendStatus(500);
        }
    }
});

async function setMessageTimer(message: Message) {
    messages.push(message);
    const deltaTime = Math.max(0, message.timestamp.getTime() - Date.now());
    console.log(`Message: "${message.text}" will be printed in ${deltaTime} ms`);
    return new Promise((resolve) => {
        const messageTimeout = setTimeout(async () => {
            console.log(`${message.text} Scheduled: ${message.timestamp.toLocaleTimeString()} Current: ${new Date().toLocaleTimeString()}`);
            resolve();
        }, deltaTime);
        timers.set(messageRedisID(message), messageTimeout);
    });
}

async function getNewMessages() {
    lastId = await redisAccessor.getLastMessageId();
    const newStreamMessages = await redisAccessor.loadMessagesFromStream(lastId);
    if (newStreamMessages && !!newStreamMessages.length) {
        await redisAccessor.addMessagesToSortedSet(newStreamMessages);
        const newLastId = newStreamMessages[newStreamMessages.length - 1].id;
        await redisAccessor.setLastMessageId(newLastId);
        lastId = newLastId;
        
        let currentMessages;
        if (isPastMessagesLoaded) {
            currentMessages = await redisAccessor.loadMessagesFromSet();
            isPastMessagesLoaded = true;
        } else {
            currentMessages = newStreamMessages;
        }
        currentMessages.forEach(async m => {
            await setMessageTimer(m);
            redisAccessor.removeMessageFromSet(m);
        });
    }
}

function observeMessages() {
    return of(null)
        .pipe(expand(() =>
            from(
                getNewMessages()
            )
        ));
}

app.listen(3000, () => {
    console.log('App is listening on port 3000');
});

redisClient.on('connect', async () => {
    console.log('Connected to Redis');
    observeMessages().subscribe();
});

redisClient.on('error', (err) => {
    console.log(`Something went wrong ${err}`);
});
