# Telegram Promo Guard Bot — Railway + PostgreSQL

Bot Telegram de modération/promo pour un groupe, prêt à déployer sur Railway.

## Changements importants

- Les admins lancent `/start` en privé et reçoivent directement le panel admin.
- `/admin` reste seulement comme secours en privé. Dans le groupe, la commande est supprimée.
- Le bouton `Je partage` crée un vrai lien personnalisé par utilisateur avec `create_chat_invite_link`.
- Le bot envoie en privé :
  - le lien personnalisé écrit, pour copier/coller ;
  - un bouton `Envoyer le lien` qui ouvre la fenêtre de partage Telegram.
- Broadcast groupe et broadcast privé sont séparés.
- Le broadcast privé marche uniquement pour les utilisateurs qui ont déjà lancé le bot en privé avec `/start`, car Telegram interdit aux bots d’écrire en premier à un utilisateur.

## Fonctions

- Supprime les messages d’entrée/sortie.
- Panel admin en boutons via `/start` en privé.
- ON/OFF pour autoriser ou bloquer les messages des utilisateurs.
- Les admins peuvent toujours envoyer des messages.
- Supprime tous les médias : photo, vidéo, document, sticker, vocal, etc.
- Si un nouveau membre envoie un média dans les 2 minutes après son arrivée : mute 1 jour.
- Liste de mots interdits ajoutable/supprimable via panel admin.
- Détection insensible aux majuscules et accents : `Pv`, `pV`, `privé`, etc.
- Liens et @ interdits pour les utilisateurs : mute 1 jour, puis 7 jours si récidive.
- Broadcast dans le groupe via panel admin.
- Broadcast en privé aux utilisateurs qui ont déjà démarré le bot.
- Message aléatoire dissuasif toutes les 2h, supprimé après 2 minutes.
- Système récompense : l’utilisateur clique `Je partage`, reçoit son lien d’invitation personnalisé, et reçoit le lien Gofile en privé après 6 arrivées via ce lien.

## Variables Railway

```env
BOT_TOKEN=token_du_bot
DATABASE_URL=postgresql://...
GROUP_ID=-100xxxxxxxxxx
ADMIN_IDS=123456789,987654321
REWARD_IMAGE_URL=https://votre-image.jpg
REWARD_REQUIRED_JOINS=6
```

## Déploiement Railway

1. Crée un bot avec `@BotFather` et récupère le token.
2. Dans `@BotFather`, désactive la privacy du bot : `Bot Settings > Group Privacy > Turn off`.
3. Ajoute le bot dans le groupe.
4. Mets le bot administrateur avec droits : supprimer messages, restreindre membres, inviter utilisateurs.
5. Crée un projet Railway.
6. Ajoute un service PostgreSQL.
7. Ajoute les variables d’environnement.
8. Déploie le repo.
9. En privé avec le bot, envoie `/start`.

## Note Telegram importante

Un bot ne peut pas envoyer de message privé à quelqu’un qui ne l’a jamais démarré. Donc, pour recevoir le lien personnalisé, le lien Gofile ou les broadcasts privés, l’utilisateur doit avoir ouvert le bot en privé au moins une fois avec `/start`.
