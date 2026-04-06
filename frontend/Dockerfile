FROM nginx:alpine

# Copy static assets
COPY static/ /usr/share/nginx/html/static/
COPY templates/index.html /usr/share/nginx/html/index.html

# Copy nginx config (passed via build context or mounted)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
