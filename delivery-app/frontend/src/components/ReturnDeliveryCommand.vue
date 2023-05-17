<template>

    <v-card outlined>
        <v-card-title>
            ReturnDelivery
        </v-card-title>

        <v-card-text>
            <Number label="Id" v-model="value.id" :editMode="editMode"/>
            <Number label="OrderId" v-model="value.orderId" :editMode="editMode"/>
        </v-card-text>

        <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn
                    color="deep-purple lighten-2"
                    text
                    @click="returnDelivery"
            >
                ReturnDelivery
            </v-btn>
            
            <v-btn
                    color="deep-purple lighten-2"
                    text
                    @click="close"
            >
                Close
            </v-btn>
        </v-card-actions>
    </v-card>

</template>

<script>
   
    export default {
        name: 'ReturnDeliveryCommand',
        components:{},
        props: {},
        data: () => ({
            editMode: true,
            value: {},
        }),
        created() {
            this.value.id = 0;
            this.value.orderId = 0;
        },
        watch: {
        },
        methods: {
            returnDelivery() {
                this.$emit('returnDelivery', this.value);
            },
            close() {
                this.$emit('closeDialog');
            },
            change() {
                this.$emit('input', this.value);
            },
        }
    }
</script>

