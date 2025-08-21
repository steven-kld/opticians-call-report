# opticians-call-report

npm i -g vercel
vercel login
vercel .

vercel.json
``` json
{
    "builds": [
        {
            "src": "api/index.py",
            "use": "@vercel/python"
        }
    ],
    "routes": [
        {
            "src": "/(.*)",
            "dest": "api/index.py"
        }
    ]
}
```

vercel env
OPENAI_API_KEY