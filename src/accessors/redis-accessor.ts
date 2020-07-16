import { Redis } from 'ioredis';
import { Message } from 'message';
import { REDIS_EVENT_STREAM, REDIS_MESSAGES_SET, REDIS_LAST_RECIEVED_ID } from '../models/consts';
import { messageRedisID, convertMessageFromRedisSet, convertMessagesFromStream } from '../utils/converters';

export class RedisAccessor {

    constructor(private redisClient: Redis) { }

    addMessageToEventStream = async (message: Message) => {
        await this.redisClient.xadd(REDIS_EVENT_STREAM, '*',
            'timestamp', message.timestamp.getTime(),
            'text', message.text)
    }

    loadMessagesFromStream = async (lastId: number) => {
        const streams = await this.redisClient.xread('COUNT', 10000, 'STREAMS', REDIS_EVENT_STREAM, lastId);
        const messages = streams ? convertMessagesFromStream(streams as unknown as EventStream[]) : [];
        return messages;
    }

    addMessageToSortedSet = async (message: Message) => {
        await this.redisClient.zadd(REDIS_MESSAGES_SET, message.timestamp.getTime(), messageRedisID(message));
    }

    addMessagesToSortedSet = async (messages: Message[]) => {
        const promises = messages.map(this.addMessageToSortedSet);
        await Promise.all(promises);
    }

    setLastMessageId = async (id: number) => {
        await this.redisClient.set(REDIS_LAST_RECIEVED_ID, id);
    }

    deleteLastMessageId = async () => {
        await this.redisClient.del(REDIS_LAST_RECIEVED_ID);
    }

    getLastMessageId = async () => {
        let id = await this.redisClient.get(REDIS_LAST_RECIEVED_ID);
        return parseInt(id) || 0;
    }

    removeMessageFromSet = async (message: Message) => {
        await this.redisClient.zrem(REDIS_MESSAGES_SET, messageRedisID(message));
    }

    loadMessagesFromSet = async () => {
        const redisMessages = await this.redisClient.zrange(REDIS_MESSAGES_SET, 0, -1)
        const messages = redisMessages.map(convertMessageFromRedisSet);
        return messages;
    }

    clearMessageStream = async () => {
        await this.redisClient.xtrim(REDIS_EVENT_STREAM, 'MAXLEN', 0);
    }

}