# Carpulse Backend

Backend service used for [CarPulse mobile application](https://github.com/renatomajer/CarPulse).

Developed with Node.js (Express) and connects to a MongoDB database and an MQTT broker to manage and store driving data and real-time events.

---

## Installation & Running

1. Clone the repository:
   ```bash
   git clone https://github.com/renatomajer/Carpulse-backend.git
   cd Carpulse-backend
   ```

2. Install dependencies:
   ```bash 
   npm install
   ```

4. Set up your environment variables.
   Create a `.env` file in the root of your project and provide all necessary variables.

5. Run the server locally
   ```bash
   npm run start
   ```
  The server should now be running on `http://localhost:4000`.


## PM2 Deployment
  On your production server you can use PM2 to keep the process alive:
  ```bash
      npm install -g pm2
      pm2 start index.js --name carpulse-backend
      pm2 save
      pm2 startup
  ```

## Using Nginx as Reverse Proxy
To serve your backend under a domain or IP, set up Nginx as a reverse proxy.
Create a config file, for example `/etc/nginx/sites-available/backend`:
```
server {
    listen 80;
    server_name yourdomain.com;  # or your server's IP address

    location / {
        proxy_pass http://localhost:4000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```
Enable and reload Nginx:
```bash
sudo ln -s /etc/nginx/sites-available/backend /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## Database & MQTT
- Connects to a MongoDB database.
- Connects to an MQTT broker.
Ensure these services are up and accessible from your server.
