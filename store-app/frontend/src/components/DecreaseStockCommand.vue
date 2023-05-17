<template>

    <v-card outlined>
        <v-card-title>
            DecreaseStock
        </v-card-title>

        <v-card-text>
            <Number label="Id" v-model="value.id" :editMode="editMode"/>
            <String label="Name" v-model="value.name" :editMode="editMode"/>
            <Number label="Stock" v-model="value.stock" :editMode="editMode"/>
            <Number label="OrderId" v-model="value.orderId" :editMode="editMode"/>
        </v-card-text>

        <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn
                    color="deep-purple lighten-2"
                    text
                    @click="decreaseStock"
            >
                DecreaseStock
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
        name: 'DecreaseStockCommand',
        components:{},
        props: {},
        data: () => ({
            editMode: true,
            value: {},
        }),
        created() {
            this.value.id = 0;
            this.value.name = '';
            this.value.stock = 0;
            this.value.orderId = 0;
        },
        watch: {
        },
        methods: {
            decreaseStock() {
                this.$emit('decreaseStock', this.value);
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

