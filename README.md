# Echo at time project

For execution you need redis running at your local port `6379`.

Run `npm install` and then `npm run start`.

Post message to `localhost:3000/echoAtTime/` with payload in format:

```json
{
    "text": "Some nice message.",
    "timestamp": 1594872441378
}
```

Have fun!